import { S3 } from 'aws-sdk';
import * as fs from 'fs';
import * as path from 'path';
import * as lz4 from 'lz4';
import { unpack } from 'msgpackr';

export namespace types {
  export interface LakeConfig {
    network: 'mainnet' | 'testnet';
    startBlockHeight: number;
    endBlockHeight?: number;
    s3RegionName?: string;
    outputDir?: string;
    follow?: boolean;
    pollInterval?: number;
  }

  export interface Block {
    hash: string;
    parentHash: string;
    number: number;
    timestamp: number;
    transactions: Transaction[];
    // Other block fields
    miner: string;
    stateRoot: string;
    transactionsRoot: string;
    receiptsRoot: string;
    sha3Uncles: string;
    gasLimit: number;
    gasUsed: number;
    baseFeePerGas: number;
  }

  export interface Transaction {
    type: string;
    chainId: number;
    nonce: number;
    gas: number;
    to: string;
    value: number;
    input: string;
    signature: any;
  }

  export interface StreamerMessage {
    block: Block;
  }
}

class HyperliquidLake {
  private s3: S3;
  private config: types.LakeConfig;
  private currentBlock: number;
  private isRunning: boolean = false;

  constructor(config: types.LakeConfig) {
    this.config = {
      ...config,
      s3RegionName: config.s3RegionName || 'us-east-1',
      outputDir: config.outputDir || './data',
      follow: config.follow || false,
      pollInterval: config.pollInterval || 60000,
    };

    this.s3 = new S3({
      region: this.config.s3RegionName,
      signatureVersion: 'v4',
    });

    this.currentBlock = this.config.startBlockHeight;

    if (this.config.outputDir && !fs.existsSync(this.config.outputDir)) {
      fs.mkdirSync(this.config.outputDir, { recursive: true });
    }
  }

  private getS3Key(blockNumber: number): string {
    const topDir = Math.floor(blockNumber / 1_000_000) * 1_000_000;
    const thousands = Math.floor(blockNumber / 1000) * 1000;
    return `${topDir}/${thousands}/${blockNumber}.rmp.lz4`;
  }

  private async blockExists(blockNumber: number): Promise<boolean> {
    const key = this.getS3Key(blockNumber);
    const bucketName = `hl-${this.config.network}-evm-blocks`;

    try {
      await this.s3
        .headObject({ Bucket: bucketName, Key: key, RequestPayer: 'requester' })
        .promise();
      return true;
    } catch (error: any) {
      return error.code !== 'NotFound' && error.code !== 'NoSuchKey';
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
    return lz4.decode(compressedData);
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

  private processTransaction(tx: any): types.Transaction {
    if (!tx.transaction) return {} as types.Transaction;

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

  private parseBlockData(blockData: any): types.Block {
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
      transactions: (rethBlock.body?.transactions || []).map((tx: any) => this.processTransaction(tx)),
    };
  }

  private processMsgpackData(data: Buffer): types.Block[] {
    try {
      const unpacked = unpack(data);
      const blocks: types.Block[] = [];
      
      if (Array.isArray(unpacked)) {
        for (const blockData of unpacked) {
          blocks.push(this.parseBlockData(blockData));
        }
      } else {
        blocks.push(this.parseBlockData(unpacked));
      }
      
      return blocks;
    } catch (error) {
      console.error('Error unpacking MessagePack data:', error);
      return [];
    }
  }

  private async findLatestBlock(): Promise<number> {
    // Start with a high block number and search backwards
    let high = 10_000_000; // Arbitrary high number
    let low = 0;
    
    // Binary search to find the latest block
    while (low <= high) {
      const mid = Math.floor((low + high) / 2);
      
      if (await this.blockExists(mid)) {
        low = mid + 1;
      } else {
        high = mid - 1;
      }
    }
    
    return high;
  }

  private async processNextBlock(callback: (message: types.StreamerMessage) => Promise<void>): Promise<boolean> {
    try {
      if (!(await this.blockExists(this.currentBlock))) {
        return false;
      }

      const compressedData = await this.downloadBlock(this.currentBlock);
      
      if (this.config.outputDir) {
        const outputLz4Path = path.join(this.config.outputDir, `${this.currentBlock}.lz4`);
        fs.writeFileSync(outputLz4Path, compressedData);
      }

      const decompressedData = await this.decompressLz4(compressedData);
      
      if (this.config.outputDir) {
        const outputMsgpackPath = path.join(this.config.outputDir, `${this.currentBlock}.msgpack`);
        fs.writeFileSync(outputMsgpackPath, decompressedData);
      }

      const blocks = this.processMsgpackData(decompressedData);
      
      for (const block of blocks) {
        await callback({ block });
      }
      
      return true;
    } catch (error) {
      console.error(`Error processing block ${this.currentBlock}:`, error);
      return false;
    }
  }

  private sleep(ms: number): Promise<void> {
    return new Promise(resolve => setTimeout(resolve, ms));
  }

  public async stop(): Promise<void> {
    this.isRunning = false;
  }

  public async startStream(callback: (message: types.StreamerMessage) => Promise<void>): Promise<void> {
    this.isRunning = true;
    
    // Find the latest block if endBlockHeight is not specified
    if (!this.config.endBlockHeight && this.config.follow) {
      this.config.endBlockHeight = await this.findLatestBlock();
    }
    
    while (this.isRunning) {
      // Process blocks up to the current endBlockHeight
      while (this.currentBlock <= (this.config.endBlockHeight || Infinity) && this.isRunning) {
        const success = await this.processNextBlock(callback);
        if (success) {
          this.currentBlock++;
        } else {
          // If we can't process the current block and we're not in follow mode, break
          if (!this.config.follow) {
            break;
          }
          // In follow mode, wait and try again
          await this.sleep(5000);
        }
      }
      
      // If we're not in follow mode or we've been stopped, break
      if (!this.config.follow || !this.isRunning) {
        break;
      }
      
      // Check for new blocks
      const latestBlock = await this.findLatestBlock();
      
      if (latestBlock > (this.config.endBlockHeight || 0)) {
        this.config.endBlockHeight = latestBlock;
      } else {
        // Wait before polling again
        await this.sleep(this.config.pollInterval || 60000);
      }
    }
  }
}

// Public API
export async function startStream(
  config: types.LakeConfig,
  callback: (message: types.StreamerMessage) => Promise<void>
): Promise<{ stop: () => Promise<void> }> {
  const lake = new HyperliquidLake(config);
  
  // Start processing in the background
  const streamPromise = lake.startStream(callback);
  
  // Return a handle that can be used to stop the stream
  return {
    stop: async () => {
      await lake.stop();
      await streamPromise;
    }
  };
}
