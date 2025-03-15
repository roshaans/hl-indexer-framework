import { S3 } from 'aws-sdk';
import * as fs from 'fs';
import * as path from 'path';
import { uncompress } from 'lz4-napi';
import { unpack } from 'msgpackr';

export interface IndexerConfig {
  network: 'mainnet' | 'testnet';
  startBlock: number;
  endBlock: number;
  outputDir: string;
  downloadOnly?: boolean;
}

export class HyperliquidIndexer {
  private s3: S3;
  private config: IndexerConfig;
  private blocks: any[] = [];

  constructor(config: IndexerConfig) {
    this.config = config;
    this.s3 = new S3({
      region: 'us-east-1', // Adjust if needed
      signatureVersion: 'v4',
    });

    // Ensure output directory exists
    if (!fs.existsSync(this.config.outputDir)) {
      fs.mkdirSync(this.config.outputDir, { recursive: true });
    }
  }

  /**
   * Get the S3 key for a specific block number
   */
  private getS3Key(blockNumber: number): string {
    if (this.config.network === 'testnet') {
      // Testnet always starts from 18,000,000
      const thousands = Math.floor(blockNumber / 1000) * 1000;
      return `18000000/${thousands}/${blockNumber}.rmp.lz4`;
    } else {
      // Mainnet format: use million-level directory first
      const topDir = Math.floor(blockNumber / 1_000_000) * 1_000_000;
      const thousands = Math.floor(blockNumber / 1000) * 1000;
      return `${topDir}/${thousands}/${blockNumber}.rmp.lz4`;
    }
  }

  /**
   * Get the full S3 path for a specific block number
   */
  private getS3Path(blockNumber: number): string {
    const bucketName = `hl-${this.config.network}-evm-blocks`;
    const key = this.getS3Key(blockNumber);

    console.debug(
      `Block ${blockNumber} S3 Path: s3://${bucketName}/${key}`
    );

    return `s3://${bucketName}/${key}`;
  }

  /**
   * Validate the S3 path format for a block
   */
  private validateS3Path(blockNumber: number): boolean {
    if (blockNumber < 0) {
      console.error(`Invalid block number: ${blockNumber}`);
      return false;
    }

    const topDir = Math.floor(blockNumber / 1_000_000) * 1_000_000;
    const thousands = Math.floor(blockNumber / 1000) * 1000;

    if (thousands < 0 || topDir < 0) {
      console.error(`Invalid computed directories for block ${blockNumber}`);
      return false;
    }

    console.debug(`Validated Block ${blockNumber}: topDir=${topDir}, thousands=${thousands}`);
    return true;
  }

  /**
   * Check if a block exists in S3
   */
  private async blockExists(blockNumber: number): Promise<boolean> {
    if (!this.validateS3Path(blockNumber)) {
      return false;
    }

    const key = this.getS3Key(blockNumber);
    const bucketName = `hl-${this.config.network}-evm-blocks`;

    try {
      await this.s3
        .headObject({
          Bucket: bucketName,
          Key: key,
          RequestPayer: 'requester',
        })
        .promise();

      console.debug(`Block ${blockNumber} exists in S3.`);
      return true;
    } catch (error: any) {
      if (error.code === 'NotFound' || error.code === 'NoSuchKey') {
        console.warn(`Block ${blockNumber} does not exist in S3.`);
        return false;
      }

      console.warn(
        `Error checking if block ${blockNumber} exists: ${error.message || error}`
      );
      return true; // Assume the block *might* exist if an unexpected error occurs
    }
  }

  /**
   * Download a block file from S3 with retries
   */
  private async downloadBlock(blockNumber: number, retries = 3): Promise<Buffer> {
    const key = this.getS3Key(blockNumber);
    const bucketName = `hl-${this.config.network}-evm-blocks`;

    const s3Path = this.getS3Path(blockNumber);
    console.log(`Downloading block ${blockNumber} from ${s3Path}`);

    let lastError;
    for (let attempt = 0; attempt < retries; attempt++) {
      try {
        const response = await this.s3.getObject({
          Bucket: bucketName,
          Key: key,
          RequestPayer: 'requester', // Important: you pay for the data transfer
        }).promise();

        return response.Body as Buffer;
      } catch (error: any) {
        lastError = error;

        // Don't retry if the block doesn't exist
        if (error.code === 'NoSuchKey') {
          const errorPath = this.getS3Path(blockNumber);
          console.error(`Block ${blockNumber} does not exist in S3 bucket (path: ${errorPath})`);
          break;
        }

        // Only retry on potentially transient errors
        if (error.retryable === false) {
          break;
        }

        console.warn(`Attempt ${attempt + 1}/${retries} failed for block ${blockNumber}. Retrying...`);
        // Add exponential backoff
        await new Promise(resolve => setTimeout(resolve, 1000 * Math.pow(2, attempt)));
      }
    }

    console.error(`Error downloading block ${blockNumber} after ${retries} attempts:`, lastError);
    throw lastError;
  }

  /**
   * Decompress LZ4 data
   */
  private async decompressLz4(compressedData: Buffer): Promise<Buffer> {
    // Use lz4-napi's uncompress function
    return await uncompress(compressedData);
  }

  /**
   * Convert a Buffer object to hex string
   */
  private convertBuffer(bufferObj: any): string {
    if (bufferObj && bufferObj.type === 'Buffer' && Array.isArray(bufferObj.data)) {
      return '0x' + Buffer.from(bufferObj.data).toString('hex');
    }
    if (Buffer.isBuffer(bufferObj)) {
      return '0x' + bufferObj.toString('hex');
    }
    return String(bufferObj);
  }

  /**
   * Convert bytes to integer
   */
  private bytesToInt(value: any): number {
    if (value && value.type === 'Buffer' && Array.isArray(value.data)) {
      return parseInt(Buffer.from(value.data).toString('hex') || '0', 16);
    }
    if (Buffer.isBuffer(value)) {
      return parseInt(value.toString('hex') || '0', 16);
    }
    return 0;
  }

  /**
   * Process nested Buffer objects
   */
  private processNestedBuffers(data: any): any {
    if (data === null || data === undefined) {
      return data;
    }

    if (data.type === 'Buffer' && Array.isArray(data.data)) {
      return this.convertBuffer(data);
    }

    if (Buffer.isBuffer(data)) {
      return this.convertBuffer(data);
    }

    if (Array.isArray(data)) {
      return data.map(item => this.processNestedBuffers(item));
    }

    if (typeof data === 'object') {
      const result: any = {};
      for (const key in data) {
        result[key] = this.processNestedBuffers(data[key]);
      }
      return result;
    }

    return data;
  }

  /**
   * Process a transaction
   */
  private processTransaction(tx: any): any {
    if (!tx.transaction) {
      return {};
    }

    const txData = tx.transaction;
    const txType = Object.keys(txData)[0]; // Either 'Legacy' or 'Eip1559'
    const txContent = txData[txType];

    const processed: any = {
      type: txType,
      chainId: this.bytesToInt(txContent.chainId),
      nonce: this.bytesToInt(txContent.nonce),
      gas: this.bytesToInt(txContent.gas),
      to: this.processNestedBuffers(txContent.to),
      value: this.bytesToInt(txContent.value),
      input: this.processNestedBuffers(txContent.input),
      signature: Array.isArray(tx.signature)
        ? tx.signature.map((sig: any) => this.processNestedBuffers(sig))
        : [],
    };

    if (txType === 'Legacy') {
      processed.gasPrice = this.bytesToInt(txContent.gasPrice);
    } else if (txType === 'Eip1559') {
      processed.maxFeePerGas = this.bytesToInt(txContent.maxFeePerGas);
      processed.maxPriorityFeePerGas = this.bytesToInt(txContent.maxPriorityFeePerGas);
      processed.accessList = this.processNestedBuffers(txContent.accessList || []);
    }

    return processed;
  }

  /**
   * Process a block
   */
  private processBlock(blockData: any): any {
    if (!blockData || !blockData.block) {
      throw new Error('Invalid block format');
    }

    const rethBlock = blockData.block.Reth115;
    const header = rethBlock?.header?.header || {};

    const processedBlock: any = {
      hash: this.processNestedBuffers(rethBlock.header?.hash),
      parentHash: this.processNestedBuffers(header.parentHash),
      sha3Uncles: this.processNestedBuffers(header.sha3Uncles),
      miner: this.processNestedBuffers(header.miner),
      stateRoot: this.processNestedBuffers(header.stateRoot),
      transactionsRoot: this.processNestedBuffers(header.transactionsRoot),
      receiptsRoot: this.processNestedBuffers(header.receiptsRoot),
      number: this.bytesToInt(header.number),
      gasLimit: this.bytesToInt(header.gasLimit),
      gasUsed: this.bytesToInt(header.gasUsed),
      timestamp: this.bytesToInt(header.timestamp),
      extraData: this.processNestedBuffers(header.extraData),
      baseFeePerGas: this.bytesToInt(header.baseFeePerGas),
      transactions: (rethBlock.body?.transactions || []).map((tx: any) => this.processTransaction(tx)),
    };

    if (processedBlock.timestamp) {
      processedBlock.datetime = new Date(processedBlock.timestamp * 1000).toISOString();
    } else {
      processedBlock.datetime = null;
    }

    return processedBlock;
  }

  /**
   * Process a MessagePack file
   */
  private processMsgpackData(data: Buffer): void {
    const unpacked = unpack(data);

    if (Array.isArray(unpacked)) {
      for (const blockData of unpacked) {
        const processedBlock = this.processBlock(blockData);
        this.blocks.push(processedBlock);
      }
    } else {
      const processedBlock = this.processBlock(unpacked);
      this.blocks.push(processedBlock);
    }
  }

  /**
   * Save processed blocks to a JSON file
   */
  private saveToJson(filename: string): void {
    const output = {
      blocks: this.blocks,
      totalBlocks: this.blocks.length,
      totalTransactions: this.blocks.reduce((sum, block) => sum + block.transactions.length, 0),
    };

    fs.writeFileSync(filename, JSON.stringify(output, null, 2));
    console.log(`Saved processed blocks to ${filename}`);
  }

  /**
   * Summarize the processed blocks
   */
  public summarizeBlocks(): any {
    if (this.blocks.length === 0) {
      return { error: 'No blocks processed' };
    }

    const totalGasUsed = this.blocks.reduce((sum, block) => sum + block.gasUsed, 0);
    const totalTxs = this.blocks.reduce((sum, block) => sum + block.transactions.length, 0);

    return {
      totalBlocks: this.blocks.length,
      totalTransactions: totalTxs,
      averageGasUsed: totalGasUsed / this.blocks.length,
      blockNumbers: this.blocks.map(block => block.number),
      timeRange: {
        first: this.blocks.find(b => b.datetime)?.datetime || null,
        last: [...this.blocks].reverse().find(b => b.datetime)?.datetime || null,
      },
    };
  }

  /**
   * Run the indexer
   */
  public async run(): Promise<void> {
    const failedBlocks: number[] = [];

    for (let height = this.config.startBlock; height <= this.config.endBlock; height++) {
      try {
        // Log the S3 path we're about to check
        const blockPath = this.getS3Path(height);
        console.log(`Checking if block ${height} exists at path: ${blockPath}`);

        // Check if the block exists before attempting to download
        const exists = await this.blockExists(height);
        if (!exists) {
          console.warn(`Skipping block ${height}: Does not exist in S3 bucket (path: ${blockPath})`);
          failedBlocks.push(height);
          continue;
        }

        // Download the block from S3
        const compressedData = await this.downloadBlock(height);
        const outputLz4Path = path.join(this.config.outputDir, `${height}.rmp.lz4`);
        fs.writeFileSync(outputLz4Path, compressedData);

        // Decompress the LZ4 data
        const decompressedData = await this.decompressLz4(compressedData);
        const outputMsgpackPath = path.join(this.config.outputDir, `${height}.rmp`);
        fs.writeFileSync(outputMsgpackPath, decompressedData);

        if (!this.config.downloadOnly) {
          // Process the MessagePack data
          this.processMsgpackData(decompressedData);
        }

        console.log(`Processed block ${height} from ${blockPath}`);
      } catch (error: any) {
        console.error(`Error processing block ${height}:`, error.message || error);
        failedBlocks.push(height);

        // Continue with the next block instead of stopping the entire process
        continue;
      }
    }

    // Report on failed blocks
    if (failedBlocks.length > 0) {
      console.warn(`Failed to process ${failedBlocks.length} blocks: ${failedBlocks.join(', ')}`);
    }

    if (!this.config.downloadOnly && this.blocks.length > 0) {
      // Save the processed blocks to a JSON file
      const outputJsonPath = path.join(this.config.outputDir, 'processed_blocks.json');
      this.saveToJson(outputJsonPath);

      // Print a summary
      console.log(this.summarizeBlocks());
    }
  }
}
