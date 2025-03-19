import { startStream, types } from '../lib/hyperliquid-lake-framework';
import * as hl from "@nktkas/hyperliquid"; // ESM & Common.js

const lakeConfig: types.LakeConfig = {
  network: 'mainnet',
  startBlockHeight: 1000822,
  follow: true,
  //outputDir: './data'
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
