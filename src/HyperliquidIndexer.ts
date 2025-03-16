import { S3 } from 'aws-sdk';
import * as fs from 'fs';
import * as path from 'path';
import * as lz4 from 'lz4';
import { unpack } from 'msgpackr';

interface Transaction {
  transaction?: {
    [key: string]: {
      chainId: any;
      nonce: any;
      gas: any;
      to: any;
      value: any;
      input: any;
    }
  };
  signature?: any;
}

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
      region: 'us-east-1',
      signatureVersion: 'v4',
    });

    if (!fs.existsSync(this.config.outputDir)) {
      fs.mkdirSync(this.config.outputDir, { recursive: true });
    }
  }

  private getS3Key(blockNumber: number): string {
    const topDir = Math.floor(blockNumber / 1_000_000) * 1_000_000;
    const thousands = Math.floor(blockNumber / 1000) * 1000;
    return `${topDir}/${thousands}/${blockNumber}.rmp.lz4`;
  }

  private getS3Path(blockNumber: number): string {
    const bucketName = `hl-${this.config.network}-evm-blocks`;
    const key = this.getS3Key(blockNumber);
    return `s3://${bucketName}/${key}`;
  }

  private async blockExists(blockNumber: number): Promise<boolean> {
    const key = this.getS3Key(blockNumber);
    const bucketName = `hl-${this.config.network}-evm-blocks`;

    try {
      console.log(`Checking if block ${blockNumber} exists in S3...`);
      await this.s3
        .headObject({ Bucket: bucketName, Key: key, RequestPayer: 'requester' })
        .promise();
      console.log(`Block ${blockNumber} exists in S3`);
      return true;
    } catch (error: any) {
      if (error.code === 'NotFound' || error.code === 'NoSuchKey') {
        console.log(`Block ${blockNumber} does not exist in S3`);
        return false;
      }
      console.error(`Error checking if block ${blockNumber} exists:`, error.code || error);
      return false;
    }
  }

  private async downloadBlock(blockNumber: number): Promise<Buffer> {
    const key = this.getS3Key(blockNumber);
    const bucketName = `hl-${this.config.network}-evm-blocks`;

    const response = await this.s3.getObject({
      Bucket: bucketName,
      Key: key,
      RequestPayer: 'requester',
    }).promise();

    return response.Body as Buffer;
  }

  private async decompressLz4(compressedData: Buffer): Promise<Buffer> {
    try {
      return lz4.decode(compressedData);
    } catch (error) {
      console.error('LZ4 decompression failed:', error);
      throw error;
    }
  }

  private convertBuffer(bufferObj: any): string {
    if (bufferObj && bufferObj.type === 'Buffer' && Array.isArray(bufferObj.data)) {
      return '0x' + Buffer.from(bufferObj.data).toString('hex');
    }
    if (Buffer.isBuffer(bufferObj)) {
      return '0x' + bufferObj.toString('hex');
    }
    return String(bufferObj);
  }

  private bytesToInt(value: any): number {
    if (value && value.type === 'Buffer' && Array.isArray(value.data)) {
      return parseInt(Buffer.from(value.data).toString('hex') || '0', 16);
    }
    if (Buffer.isBuffer(value)) {
      return parseInt(value.toString('hex') || '0', 16);
    }
    return 0;
  }

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

  private processTransaction(tx: any): any {
    if (!tx.transaction) return {};

    const txData = tx.transaction;
    const txType = Object.keys(txData)[0];
    const txContent = txData[txType];

    return {
      type: txType,
      chainId: this.bytesToInt(txContent.chainId),
      nonce: this.bytesToInt(txContent.nonce),
      gas: this.bytesToInt(txContent.gas),
      to: this.processNestedBuffers(txContent.to),
      value: this.bytesToInt(txContent.value),
      input: this.processNestedBuffers(txContent.input),
      signature: tx.signature ? this.processNestedBuffers(tx.signature) : [],
    };
  }

  private processBlock(blockData: any): any {
    if (!blockData || !blockData.block) throw new Error('Invalid block format');

    const rethBlock = blockData.block.Reth115;
    const header = rethBlock?.header?.header || {};

    return {
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
      baseFeePerGas: this.bytesToInt(header.baseFeePerGas),
      transactions: (rethBlock.body?.transactions || []).map((tx: Transaction) => this.processTransaction(tx)),
    };
  }

  private processMsgpackData(data: Buffer): void {
    try {
      const unpacked = unpack(data);
      if (Array.isArray(unpacked)) {
        for (const blockData of unpacked) {
          this.blocks.push(this.processBlock(blockData));
        }
      } else {
        this.blocks.push(this.processBlock(unpacked));
      }
    } catch (error) {
      console.error('Error unpacking MessagePack data:', error);
    }
  }

  private saveBlockToJson(blockNumber: number, blockData: any): void {
    const jsonOutputPath = path.join(this.config.outputDir, `${blockNumber}.json`);
    fs.writeFileSync(jsonOutputPath, JSON.stringify(blockData, null, 2));
    console.log(`Saved block ${blockNumber} to ${jsonOutputPath}`);
    
    // Save transactions to a separate file if there are any
    if (blockData.transactions && blockData.transactions.length > 0) {
      const txOutputPath = path.join(this.config.outputDir, `${blockNumber}_transactions.json`);
      fs.writeFileSync(txOutputPath, JSON.stringify(blockData.transactions, null, 2));
      console.log(`Saved ${blockData.transactions.length} transactions from block ${blockNumber} to ${txOutputPath}`);
    }
  }

  public async run(): Promise<void> {
    const failedBlocks: number[] = [];

    for (let height = this.config.startBlock; height <= this.config.endBlock; height++) {
      console.log(`Processing block ${height}...`);
      try {
        if (!(await this.blockExists(height))) {
          console.warn(`Skipping block ${height}: Does not exist in S3`);
          failedBlocks.push(height);
          continue;
        }

        console.log(`Downloading block ${height}...`);
        const compressedData = await this.downloadBlock(height);
        const outputLz4Path = path.join(this.config.outputDir, `${height}.lz4`);
        fs.writeFileSync(outputLz4Path, compressedData);
        console.log(`Saved compressed block to ${outputLz4Path}`);

        let decompressedData;
        try {
          console.log(`Decompressing block ${height}...`);
          decompressedData = await this.decompressLz4(compressedData);
          const outputMsgpackPath = path.join(this.config.outputDir, `${height}.msgpack`);
          fs.writeFileSync(outputMsgpackPath, decompressedData);
          console.log(`Saved decompressed block to ${outputMsgpackPath}`);

          if (!this.config.downloadOnly) {
            console.log(`Processing block ${height} data...`);
            const previousLength = this.blocks.length;
            this.processMsgpackData(decompressedData);
            
            // Get the newly added blocks
            const newBlocks = this.blocks.slice(previousLength);
            for (const block of newBlocks) {
              console.log(`Block ${height} has ${block.transactions?.length || 0} transactions`);
              this.saveBlockToJson(height, block);
            }
          }
        } catch (decompressError) {
          console.error(`Failed to decompress block ${height}:`, decompressError);
          continue;
        }
      } catch (error) {
        console.error(`Error processing block ${height}:`, error);
        failedBlocks.push(height);
        continue;
      }
    }

    if (failedBlocks.length > 0) {
      console.warn(`Failed to process ${failedBlocks.length} blocks: ${failedBlocks.join(', ')}`);
    }
  }
}
