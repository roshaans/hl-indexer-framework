import { startStream, types } from '../lib/hyperliquid-lake-framework';
import * as fs from 'fs';
import * as path from 'path';

// Configuration
const lakeConfig: types.LakeConfig = {
  network: 'mainnet',
  startBlockHeight: 531166682, // Starting block
  follow: true,               // Follow the chain tip
  //outputDir: './data'
};

// The address to track (replace with the address you want to monitor)
const TARGET_ADDRESS = '0x4A5498a53D97B6E7A5c0b0Ff2d431FD953815a80'.toLowerCase();

// Store transactions by address
interface AddressActivity {
  address: string;
  sent: types.Transaction[];
  received: types.Transaction[];
  lastUpdated: number;
}

// Initialize activity tracking
const addressActivity: Record<string, AddressActivity> = {};

// Process each block and look for transactions involving the target address
async function handleStreamerMessage(message: types.StreamerMessage): Promise<void> {
  const block = message.block;
  console.log(`Processing block #${block.number} with ${block.transactions.length} transactions`);

  let userActivityFound = false;

  // Process each transaction in the block
  for (const tx of block.transactions) {
    const from = tx.signature?.signer?.toLowerCase() || '';
    const to = tx.to?.toLowerCase() || '';

    // Check if transaction involves our target address
    if (from === TARGET_ADDRESS || to === TARGET_ADDRESS) {
      userActivityFound = true;

      console.log(`Found transaction involving target address in block ${block.number}:`);
      console.log(`  From: ${from}`);
      console.log(`  To: ${to}`);
      console.log(`  Value: ${tx.value}`);
      console.log(`  Type: ${tx.type}`);

      // Track sent transactions
      if (from === TARGET_ADDRESS) {
        if (!addressActivity[TARGET_ADDRESS]) {
          addressActivity[TARGET_ADDRESS] = {
            address: TARGET_ADDRESS,
            sent: [],
            received: [],
            lastUpdated: block.timestamp
          };
        }

        addressActivity[TARGET_ADDRESS].sent.push(tx);
        addressActivity[TARGET_ADDRESS].lastUpdated = block.timestamp;
      }

      // Track received transactions
      if (to === TARGET_ADDRESS) {
        if (!addressActivity[TARGET_ADDRESS]) {
          addressActivity[TARGET_ADDRESS] = {
            address: TARGET_ADDRESS,
            sent: [],
            received: [],
            lastUpdated: block.timestamp
          };
        }

        addressActivity[TARGET_ADDRESS].received.push(tx);
        addressActivity[TARGET_ADDRESS].lastUpdated = block.timestamp;
      }

      const activity = addressActivity[TARGET_ADDRESS];
      console.log(activity)
      // Save updated activity to file
      //saveActivityToFile();
    }
  }

  // Log a summary if we found activity
  if (userActivityFound) {
    const activity = addressActivity[TARGET_ADDRESS];
    console.log(`\nUser Activity Summary for ${TARGET_ADDRESS}:`);
    console.log(`  Total sent transactions: ${activity?.sent.length || 0}`);
    console.log(`  Total received transactions: ${activity?.received.length || 0}`);
    console.log(`  Last activity: ${new Date(activity?.lastUpdated * 1000).toISOString()}\n`);
  }
}

//// Save activity data to a JSON file
//function saveActivityToFile(): void {
//  const outputDir = './data/user-activity';
//
//  // Create directory if it doesn't exist
//  if (!fs.existsSync(outputDir)) {
//    fs.mkdirSync(outputDir, { recursive: true });
//  }
//
//  // Save activity for the target address
//  const activity = addressActivity[TARGET_ADDRESS];
//  if (activity) {
//    const filePath = path.join(outputDir, `${TARGET_ADDRESS}.json`);
//    fs.writeFileSync(filePath, JSON.stringify(activity, null, 2));
//    console.log(`Saved activity data to ${filePath}`);
//  }
//}

// Main function
(async () => {
  console.log(`Starting to monitor activity for address: ${TARGET_ADDRESS}`);
  console.log("Press Ctrl+C to stop");

  const stream = await startStream(lakeConfig, handleStreamerMessage);
})();
