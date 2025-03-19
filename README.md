# Hyperliquid Lake Framework

A simple framework for processing Hyperliquid blockchain data from S3, inspired by NEAR Lake Framework.

## Installation

```bash
npm install
```

## Usage

Create a new file with the following code:

```typescript
import { startStream, types } from './hyperliquid-lake-framework';

const lakeConfig: types.LakeConfig = {
  network: 'mainnet',
  startBlockHeight: 1000822,
  follow: true,
  outputDir: './data'
};

async function handleStreamerMessage(message: types.StreamerMessage): Promise<void> {
  console.log(`Block #${message.block.number} with ${message.block.transactions.length} transactions`);
  
  // Process transactions
  for (const tx of message.block.transactions) {
    console.log(`  Transaction to: ${tx.to}, value: ${tx.value}`);
  }
}

(async () => {
  console.log("Starting Hyperliquid Lake Framework...");
  const stream = await startStream(lakeConfig, handleStreamerMessage);
  
  // Handle graceful shutdown
  process.on('SIGINT', async () => {
    console.log("Stopping stream...");
    await stream.stop();
    console.log("Stream stopped");
    process.exit(0);
  });
})();
```

## Configuration Options

The `LakeConfig` interface supports the following options:

- `network`: 'mainnet' | 'testnet' - The Hyperliquid network to use
- `startBlockHeight`: number - The block height to start processing from
- `endBlockHeight?`: number - Optional end block height (if not specified, will process until the end)
- `s3RegionName?`: string - AWS S3 region (default: 'us-east-1')
- `outputDir?`: string - Directory to save raw block data (default: './data')
- `follow?`: boolean - Whether to follow the tip of the chain (default: false)
- `pollInterval?`: number - Milliseconds to wait between polling for new blocks (default: 60000)

## Running the Examples

Basic block processing example:
```bash
npm run example
```

User activity tracking example:
```bash
npm run user-activity
```

## Building the Framework

```bash
npm run build
```

## User Activity Example

The user activity example demonstrates how to track transactions for a specific Ethereum address:

```typescript
import { startStream, types } from './hyperliquid-lake-framework';

// Configuration
const lakeConfig: types.LakeConfig = {
  network: 'mainnet',
  startBlockHeight: 1000822,
  follow: true
};

// The address to track
const TARGET_ADDRESS = '0x7E5F4552091A69125d5DfCb7b8C2659029395Bdf'.toLowerCase();

async function handleStreamerMessage(message: types.StreamerMessage): Promise<void> {
  // Process each transaction in the block
  for (const tx of message.block.transactions) {
    const from = tx.signature?.signer?.toLowerCase() || '';
    const to = tx.to?.toLowerCase() || '';
    
    // Check if transaction involves our target address
    if (from === TARGET_ADDRESS || to === TARGET_ADDRESS) {
      console.log(`Found transaction involving target address in block ${message.block.number}`);
      // Process the transaction...
    }
  }
}

// Start monitoring
const stream = await startStream(lakeConfig, handleStreamerMessage);
```

This example saves all transactions involving the target address to a JSON file in the `data/user-activity` directory.
