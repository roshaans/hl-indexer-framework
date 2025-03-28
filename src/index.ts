import { S3Client, ListObjectsV2Command, GetObjectCommand } from '@aws-sdk/client-s3';
import * as fs from 'fs';
import * as path from 'path';
import * as child_process from 'child_process';
import { promisify } from 'util';
import { Readable } from 'stream';

const exec = promisify(child_process.exec);

// Helper function to decompress LZ4 data using temp files and the unlz4 command
async function decompressLz4(inputBuffer: Buffer, tempPrefix: string = 'temp_'): Promise<Buffer> {
  // Create a temporary directory if it doesn't exist
  const tempDir = path.join(process.cwd(), 'temp');
  if (!fs.existsSync(tempDir)) {
    fs.mkdirSync(tempDir);
  }
  
  // Create unique temp file names
  const timestamp = Date.now();
  const compressedPath = path.join(tempDir, `${tempPrefix}${timestamp}.lz4`);
  const decompressedPath = path.join(tempDir, `${tempPrefix}${timestamp}`);
  
  try {
    // Write the compressed data to a temp file
    fs.writeFileSync(compressedPath, inputBuffer);
    
    // Decompress using unlz4 command
    await exec(`unlz4 ${compressedPath} -f`);
    
    // Read the decompressed data
    const decompressedData = fs.readFileSync(decompressedPath);
    
    // Clean up temp files
    fs.unlinkSync(compressedPath);
    fs.unlinkSync(decompressedPath);
    
    return decompressedData;
  } catch (error) {
    console.error('Error decompressing with unlz4:', error);
    throw new Error('Failed to decompress LZ4 data. Make sure unlz4 is installed.');
  }
}

interface BlockInfo {
  timestamp: number;
  number: number;
  hash: string;
}

interface BufferObject {
  type: string;
  data: number[];
}

async function followNetworkTip() {
  // Initialize S3 client with AWS SDK v3
  const s3Client = new S3Client({
    region: 'ap-northeast-1', // Updated region based on error message
  });

  // Set up parameters for listing objects
  const listParams = {
    Bucket: 'hl-mainnet-node-data',
    Prefix: 'replica_cmds/',
    MaxKeys: 1000, // Increased to ensure we get enough files
    RequestPayer: 'requester' as const
  };

  let lastProcessedBlock: BlockInfo | null = null;
  
  // Function to list files in the replica_cmds directory
  async function listReplicaCmdsFiles() {
    try {
      console.log("Listing files in replica_cmds directory...");
      const command = new ListObjectsV2Command(listParams);
      const data = await s3Client.send(command);
      
      if (!data.Contents || data.Contents.length === 0) {
        console.log('No files found');
        return;
      }
      
      console.log(`Found ${data.Contents.length} files in replica_cmds/`);
      
      // Group files by date directory
      const filesByDate = new Map<string, any[]>();
      
      for (const file of data.Contents) {
        if (!file.Key) continue;
        
        // Extract date from path like replica_cmds/2025-01-26T12:18:22Z/20250202/486340000.lz4
        const pathParts = file.Key.split('/');
        if (pathParts.length >= 3) {
          const dateDir = pathParts[1]; // e.g., 2025-01-26T12:18:22Z
          
          if (!filesByDate.has(dateDir)) {
            filesByDate.set(dateDir, []);
          }
          
          filesByDate.get(dateDir)?.push(file);
        }
      }
      
      // Print summary of files by date
      console.log("\n=== Files by Date Directory ===");
      for (const [dateDir, files] of filesByDate.entries()) {
        console.log(`\n${dateDir}: ${files.length} files`);
        
        // Sort files by LastModified
        const sortedFiles = files.sort((a, b) => 
          (b.LastModified?.getTime() || 0) - (a.LastModified?.getTime() || 0)
        );
        
        // Show the 5 most recent files in each directory
        for (let i = 0; i < Math.min(5, sortedFiles.length); i++) {
          const file = sortedFiles[i];
          console.log(`  - ${file.Key} (${file.Size} bytes, Last modified: ${file.LastModified})`);
        }
      }
      
      // Get the most recent file across all directories
      const allFiles = Array.from(filesByDate.values()).flat();
      const sortedFiles = allFiles.sort((a, b) => 
        (b.LastModified?.getTime() || 0) - (a.LastModified?.getTime() || 0)
      );
      
      const latestFile = sortedFiles[0];
      console.log("\n=== Most Recent File ===");
      console.log(`Latest file: ${latestFile.Key} (Last modified: ${latestFile.LastModified})`);
      console.log(`File ETag: ${latestFile.ETag}, Size: ${latestFile.Size} bytes`);
      
      return latestFile;
    } catch (error) {
      console.error('Error listing files:', error);
      return null;
    }
  }

  // Function to process new blocks
  async function checkForNewBlocks() {
    try {
      // List files and get the most recent one
      const latestFile = await listReplicaCmdsFiles();
      
      if (!latestFile) {
        console.log('No latest file found');
        return;
      }
      
      // Skip if we've already processed this file
      if (lastProcessedBlock && lastProcessedBlock.hash === latestFile.ETag) {
        console.log('Already processed this file, skipping');
        return;
      }
      
      // Download and parse the file
      if (!latestFile.Key) {
        console.log('Latest file has no Key property');
        return;
      }
      
      const downloadParams = {
        Bucket: 'hl-mainnet-node-data',
        Key: latestFile.Key,
        RequestPayer: 'requester' as const
      };
      
      console.log(`Downloading file: ${latestFile.Key}...`);
      const getCommand = new GetObjectCommand(downloadParams);
      const fileResponse = await s3Client.send(getCommand);
      
      if (!fileResponse.Body) {
        console.log('No data in file');
        return;
      }
      
      // Convert the readable stream to a string
      const streamToString = async (stream: any): Promise<string> => {
        const chunks: Buffer[] = [];
        return new Promise((resolve, reject) => {
          stream.on('data', (chunk: Buffer) => chunks.push(chunk));
          stream.on('error', reject);
          stream.on('end', () => resolve(Buffer.concat(chunks).toString('utf-8')));
        });
      };
      
      const bodyContents = await streamToString(fileResponse.Body);
      
      // Parse the JSON data
      let blockData;
      try {
        blockData = JSON.parse(bodyContents);
        console.log('Successfully parsed block data');
        
        // Log the structure of the block data to help with debugging
        console.log('Block data structure:', JSON.stringify({
          hasTimestamp: !!blockData.timestamp,
          hasNumber: !!blockData.number,
          hasBlock: !!blockData.block,
          blockKeys: blockData.block ? Object.keys(blockData.block) : [],
          reth115: blockData.block?.Reth115 ? 'exists' : 'not found',
          headerStructure: blockData.block?.Reth115?.header ? 
            Object.keys(blockData.block.Reth115.header) : []
        }, null, 2));
      } catch (parseError) {
        console.error('Error parsing JSON data:', parseError);
        return;
      }
      
      // Extract block information
      const blockInfo: BlockInfo = {
        timestamp: blockData.timestamp || blockData.block?.Reth115?.header?.header?.timestamp || 0,
        number: blockData.number || blockData.block?.Reth115?.header?.header?.number || 0,
        hash: latestFile.ETag || ''
      };
      
      // Convert number from Buffer if needed
      if (typeof blockInfo.number === 'object' && (blockInfo.number as any)?.type === 'Buffer') {
        const bufferObj = blockInfo.number as unknown as BufferObject;
        blockInfo.number = parseInt(Buffer.from(bufferObj.data).toString('hex'), 16);
      }
      
      // Convert timestamp from Buffer if needed
      if (typeof blockInfo.timestamp === 'object' && (blockInfo.timestamp as any)?.type === 'Buffer') {
        const bufferObj = blockInfo.timestamp as unknown as BufferObject;
        blockInfo.timestamp = parseInt(Buffer.from(bufferObj.data).toString('hex'), 16);
      }
      
      // Skip if this is the same block we already processed
      if (lastProcessedBlock && lastProcessedBlock.number === blockInfo.number) {
        return;
      }
      
      // Calculate latency
      const currentTime = Math.floor(Date.now() / 1000); // Current time in seconds
      const blockTime = blockInfo.timestamp;
      const latencySeconds = currentTime - blockTime;
      
      console.log(`\n=== NEW BLOCK DETECTED ===`);
      console.log(`Block #${blockInfo.number}`);
      console.log(`Block timestamp: ${new Date(blockTime * 1000).toISOString()}`);
      console.log(`Current time: ${new Date(currentTime * 1000).toISOString()}`);
      console.log(`Latency: ${latencySeconds} seconds`);
      console.log(`===========================\n`);
      
      // Update last processed block
      lastProcessedBlock = blockInfo;
      
    } catch (error) {
      console.error('Error:', error);
    }
  }
  
  // Initial check
  await checkForNewBlocks();
  
  // Set up polling interval (every 5 seconds)
  console.log('Starting to follow the network tip. Press Ctrl+C to stop.');
  setInterval(checkForNewBlocks, 5000);
}

// Add a command line argument parser
async function inspectFile(filePath: string) {
  const s3Client = new S3Client({
    region: 'ap-northeast-1',
  });
  
  const downloadParams = {
    Bucket: 'hl-mainnet-node-data',
    Key: filePath,
    RequestPayer: 'requester' as const
  };
  
  try {
    console.log(`Downloading file: ${filePath}...`);
    const getCommand = new GetObjectCommand(downloadParams);
    const fileResponse = await s3Client.send(getCommand);
    
    if (!fileResponse.Body) {
      console.log('No data in file');
      return;
    }
    
    // Convert the readable stream to a buffer
    const streamToBuffer = async (stream: any): Promise<Buffer> => {
      const chunks: Buffer[] = [];
      return new Promise((resolve, reject) => {
        stream.on('data', (chunk: Buffer) => chunks.push(chunk));
        stream.on('error', reject);
        stream.on('end', () => resolve(Buffer.concat(chunks)));
      });
    };
    
    const bodyBuffer = await streamToBuffer(fileResponse.Body);
    
    // Check if the file is LZ4 compressed (most replica_cmds files are)
    if (filePath.endsWith('.lz4')) {
      console.log('File is LZ4 compressed. Attempting to decompress...');
      console.log(`Compressed file size: ${bodyBuffer.length} bytes`);
      
      try {
        // Decompress using unlz4 command
        console.log('Decompressing with unlz4...');
        const decompressedBuffer = await decompressLz4(bodyBuffer, 'inspect_');
        
        console.log(`Decompressed size: ${decompressedBuffer.length} bytes`);
        
        // Try to parse as JSON
        try {
          const jsonData = JSON.parse(decompressedBuffer.toString('utf-8'));
          console.log('Successfully parsed decompressed data as JSON:');
          
          // Print a summary of the structure
          const summary = {
            type: typeof jsonData,
            isArray: Array.isArray(jsonData),
            topLevelKeys: typeof jsonData === 'object' ? Object.keys(jsonData) : [],
            sampleData: typeof jsonData === 'object' ? 
              JSON.stringify(jsonData).substring(0, 1000) + '...' : 
              jsonData.toString().substring(0, 1000) + '...'
          };
          
          console.log('Data structure summary:', JSON.stringify(summary, null, 2));
          
          // If it's an array, show the first few items
          if (Array.isArray(jsonData)) {
            console.log(`Array length: ${jsonData.length}`);
            if (jsonData.length > 0) {
              console.log('First item sample:');
              console.log(JSON.stringify(jsonData[0], null, 2));
            }
          } 
          // If it's an object, show its structure
          else if (typeof jsonData === 'object' && jsonData !== null) {
            console.log('Data structure summary:');
            console.log(JSON.stringify(Object.keys(jsonData), null, 2));
            
            // Try to extract transactions
            extractTransactions(jsonData);
            
            // Show a limited view of the full data
            console.log('\nFull data preview (limited to first 5000 chars):');
            console.log(JSON.stringify(jsonData, null, 2).substring(0, 5000) + '...');
          }
          
        } catch (jsonError) {
          console.log('Decompressed data is not valid JSON. Showing first 2000 bytes:');
          console.log(decompressedBuffer.toString('utf-8', 0, 2000));
          
          // Save to a file for further inspection
          const outputFileName = path.basename(filePath, '.lz4') + '.decompressed';
          fs.writeFileSync(outputFileName, decompressedBuffer);
          console.log(`Saved decompressed data to ${outputFileName} for further inspection`);
        }
      } catch (decompressError) {
        console.error('Error decompressing LZ4 file:', decompressError);
        console.log('First 100 bytes of compressed data (hex):');
        console.log(bodyBuffer.slice(0, 100).toString('hex'));
      }
      
      return;
    }
    
    // Try to parse as JSON if not LZ4
    try {
      const jsonData = JSON.parse(bodyBuffer.toString('utf-8'));
      console.log('File contents (JSON):');
      console.log(JSON.stringify(jsonData, null, 2).slice(0, 5000) + '...');
    } catch (e) {
      console.log('File is not valid JSON. Showing first 1000 bytes:');
      console.log(bodyBuffer.toString('utf-8', 0, 1000));
    }
  } catch (error) {
    console.error('Error inspecting file:', error);
  }
}

async function downloadAndDecompress(filePath: string, outputPath?: string) {
  const s3Client = new S3Client({
    region: 'ap-northeast-1',
  });
  
  const downloadParams = {
    Bucket: 'hl-mainnet-node-data',
    Key: filePath,
    RequestPayer: 'requester' as const
  };
  
  try {
    console.log(`Downloading file: ${filePath}...`);
    const getCommand = new GetObjectCommand(downloadParams);
    const fileResponse = await s3Client.send(getCommand);
    
    if (!fileResponse.Body) {
      console.log('No data in file');
      return;
    }
    
    // Convert the readable stream to a buffer
    const streamToBuffer = async (stream: any): Promise<Buffer> => {
      const chunks: Buffer[] = [];
      return new Promise((resolve, reject) => {
        stream.on('data', (chunk: Buffer) => chunks.push(chunk));
        stream.on('error', reject);
        stream.on('end', () => resolve(Buffer.concat(chunks)));
      });
    };
    
    const bodyBuffer = await streamToBuffer(fileResponse.Body);
    console.log(`Downloaded ${bodyBuffer.length} bytes`);
    
    // Determine output filename
    const fileName = path.basename(filePath);
    const outputFileName = outputPath || fileName.replace('.lz4', '');
    
    if (filePath.endsWith('.lz4')) {
      console.log('File is LZ4 compressed. Decompressing...');
      
      try {
        // Decompress using unlz4 command
        console.log('Decompressing with unlz4...');
        const decompressedBuffer = await decompressLz4(bodyBuffer, 'download_');
        
        console.log(`Decompressed ${bodyBuffer.length} bytes to ${decompressedBuffer.length} bytes`);
        
        // Save the decompressed data
        fs.writeFileSync(outputFileName, decompressedBuffer);
        console.log(`Saved decompressed data to ${outputFileName}`);
      } catch (decompressError) {
        console.error('Error decompressing LZ4 file:', decompressError);
        
        // Save the compressed data as-is
        fs.writeFileSync(outputFileName + '.compressed', bodyBuffer);
        console.log(`Saved compressed data to ${outputFileName}.compressed`);
      }
    } else {
      // Save the file as-is
      fs.writeFileSync(outputFileName, bodyBuffer);
      console.log(`Saved file to ${outputFileName}`);
    }
  } catch (error) {
    console.error('Error downloading file:', error);
  }
}

// Function to extract and display transactions from block data
function extractTransactions(blockData: any): void {
  try {
    console.log("\n=== Extracting Transactions ===");
    
    // Check if this is a Reth115 block format
    if (blockData.block?.Reth115) {
      const block = blockData.block.Reth115;
      
      // Extract transactions
      if (block.transactions && Array.isArray(block.transactions)) {
        console.log(`Found ${block.transactions.length} transactions`);
        
        // Display first 5 transactions
        for (let i = 0; i < Math.min(5, block.transactions.length); i++) {
          const tx = block.transactions[i];
          console.log(`\nTransaction #${i + 1}:`);
          
          // Extract common transaction fields
          const from = tx.from || tx.signature?.signer || 'unknown';
          const to = tx.to || 'contract creation';
          const value = tx.value || '0';
          
          console.log(`  From: ${from}`);
          console.log(`  To: ${to}`);
          console.log(`  Value: ${value}`);
          console.log(`  Gas: ${tx.gas || 'unknown'}`);
          
          // Show transaction hash if available
          if (tx.hash) {
            console.log(`  Hash: ${tx.hash}`);
          }
        }
        
        if (block.transactions.length > 5) {
          console.log(`\n... and ${block.transactions.length - 5} more transactions`);
        }
      } else {
        console.log("No transactions found in Reth115 format");
      }
    } 
    // Check for other block formats
    else if (blockData.transactions && Array.isArray(blockData.transactions)) {
      console.log(`Found ${blockData.transactions.length} transactions in root format`);
      
      // Display first 5 transactions
      for (let i = 0; i < Math.min(5, blockData.transactions.length); i++) {
        const tx = blockData.transactions[i];
        console.log(`\nTransaction #${i + 1}:`);
        console.log(JSON.stringify(tx, null, 2));
      }
      
      if (blockData.transactions.length > 5) {
        console.log(`\n... and ${blockData.transactions.length - 5} more transactions`);
      }
    } else {
      console.log("No transactions found in expected format");
      console.log("Available top-level keys:", Object.keys(blockData));
    }
  } catch (error) {
    console.error("Error extracting transactions:", error);
  }
}

// Function to download just the header of a file (first few KB)
async function downloadFileHeader(filePath: string, sizeInKB: number = 64) {
  const s3Client = new S3Client({
    region: 'ap-northeast-1',
  });
  
  const downloadParams = {
    Bucket: 'hl-mainnet-node-data',
    Key: filePath,
    RequestPayer: 'requester' as const,
    Range: `bytes=0-${sizeInKB * 1024 - 1}` // Download only the first X KB
  };
  
  try {
    console.log(`Downloading first ${sizeInKB} KB of file: ${filePath}...`);
    const getCommand = new GetObjectCommand(downloadParams);
    const fileResponse = await s3Client.send(getCommand);
    
    if (!fileResponse.Body) {
      console.log('No data in file');
      return;
    }
    
    // Convert the readable stream to a buffer
    const streamToBuffer = async (stream: Readable): Promise<Buffer> => {
      const chunks: Buffer[] = [];
      return new Promise((resolve, reject) => {
        stream.on('data', (chunk: Buffer) => chunks.push(chunk));
        stream.on('error', reject);
        stream.on('end', () => resolve(Buffer.concat(chunks)));
      });
    };
    
    const headerBuffer = await streamToBuffer(fileResponse.Body as Readable);
    console.log(`Downloaded ${headerBuffer.length} bytes of header data`);
    
    if (filePath.endsWith('.lz4')) {
      console.log('File is LZ4 compressed. Attempting to analyze header...');
      
      // Display the first 100 bytes as hex
      console.log('First 100 bytes (hex):');
      console.log(headerBuffer.slice(0, 100).toString('hex'));
      
      try {
        // Try to decompress the header using unlz4
        console.log('Decompressing header with unlz4...');
        const decompressedBuffer = await decompressLz4(headerBuffer, 'header_');
        
        console.log(`Decompressed ${headerBuffer.length} bytes to ${decompressedBuffer.length} bytes`);
        
        // Try to parse as JSON
        try {
          // Look at the first few bytes to determine if it's JSON
          const firstFewBytes = decompressedBuffer.slice(0, Math.min(100, decompressedBuffer.length)).toString('utf-8');
          console.log('First few bytes of decompressed data:');
          console.log(firstFewBytes);
          
          if (firstFewBytes.trim().startsWith('{') || firstFewBytes.trim().startsWith('[')) {
            // Looks like JSON, try to parse a small sample
            const jsonSample = decompressedBuffer.toString('utf-8', 0, Math.min(1000, decompressedBuffer.length));
            console.log('Sample appears to be JSON. First 1000 characters:');
            console.log(jsonSample);
          } else {
            console.log('Decompressed data does not appear to be JSON');
            console.log('First 1000 bytes as UTF-8:');
            console.log(decompressedBuffer.toString('utf-8', 0, Math.min(1000, decompressedBuffer.length)));
          }
        } catch (jsonError) {
          console.log('Error parsing decompressed data as JSON');
        }
        
        // Save the header to a file for further inspection
        const outputFileName = path.basename(filePath, '.lz4') + '.header';
        fs.writeFileSync(outputFileName, decompressedBuffer);
        console.log(`Saved decompressed header to ${outputFileName} for further inspection`);
        
      } catch (decompressError) {
        console.error('Error decompressing LZ4 header:', decompressError);
      }
    } else {
      // Try to parse as JSON if not LZ4
      try {
        const jsonData = JSON.parse(headerBuffer.toString('utf-8'));
        console.log('Header contents (JSON):');
        console.log(JSON.stringify(jsonData, null, 2).slice(0, 1000) + '...');
      } catch (e) {
        console.log('Header is not valid JSON. Showing first 1000 bytes:');
        console.log(headerBuffer.toString('utf-8', 0, 1000));
      }
    }
  } catch (error) {
    console.error('Error downloading file header:', error);
  }
}

// Function to download, decompress with unlz4 command line tool, and display file
async function downloadAndDecompressWithUnlz4(filePath: string) {
  const s3Client = new S3Client({
    region: 'ap-northeast-1',
  });
  
  const downloadParams = {
    Bucket: 'hl-mainnet-node-data',
    Key: filePath,
    RequestPayer: 'requester' as const
  };
  
  try {
    // Create a temporary directory if it doesn't exist
    const tempDir = path.join(process.cwd(), 'temp');
    if (!fs.existsSync(tempDir)) {
      fs.mkdirSync(tempDir);
    }
    
    // Generate file paths
    const fileName = path.basename(filePath);
    const compressedFilePath = path.join(tempDir, fileName);
    const decompressedFilePath = path.join(tempDir, fileName.replace('.lz4', ''));
    
    console.log(`Downloading file: ${filePath}...`);
    const getCommand = new GetObjectCommand(downloadParams);
    const fileResponse = await s3Client.send(getCommand);
    
    if (!fileResponse.Body) {
      console.log('No data in file');
      return;
    }
    
    // Save the compressed file to disk
    const writeStream = fs.createWriteStream(compressedFilePath);
    
    await new Promise((resolve, reject) => {
      (fileResponse.Body as Readable).pipe(writeStream)
        .on('finish', resolve)
        .on('error', reject);
      
      // Report progress
      let totalBytes = 0;
      let lastReportTime = Date.now();
      
      (fileResponse.Body as Readable).on('data', (chunk) => {
        totalBytes += chunk.length;
        
        // Report progress every 5 seconds
        const now = Date.now();
        if (now - lastReportTime > 5000) {
          console.log(`Download progress: ${(totalBytes / (1024 * 1024)).toFixed(2)} MB`);
          lastReportTime = now;
        }
      });
    });
    
    console.log(`Downloaded file to ${compressedFilePath}`);
    
    // Decompress using unlz4 command line tool
    console.log('Decompressing with unlz4...');
    try {
      await exec(`unlz4 ${compressedFilePath} -f`);
      console.log(`Decompressed file to ${decompressedFilePath}`);
      
      // Read and display the first part of the file
      const fileContent = fs.readFileSync(decompressedFilePath, 'utf8');
      const previewLength = Math.min(fileContent.length, 10000);
      
      console.log(`\nFile preview (first ${previewLength} characters):`);
      console.log(fileContent.substring(0, previewLength));
      
      // Try to parse as JSON
      try {
        const jsonData = JSON.parse(fileContent);
        console.log('\nSuccessfully parsed as JSON');
        
        // Extract transactions if available
        extractTransactions(jsonData);
      } catch (jsonError) {
        console.log('\nFile is not valid JSON or is too large to parse in memory');
      }
      
      console.log(`\nFull decompressed file is available at: ${decompressedFilePath}`);
      return decompressedFilePath;
    } catch (error) {
      console.error('Error running unlz4:', error);
      console.log('Make sure unlz4 is installed. You can install it with:');
      console.log('  brew install lz4    # on macOS');
      console.log('  apt install lz4     # on Ubuntu/Debian');
      console.log('  yum install lz4     # on CentOS/RHEL');
    }
  } catch (error) {
    console.error('Error downloading or processing file:', error);
  }
}

async function main() {
  const args = process.argv.slice(2);
  
  // Check for unlz4 command
  if (args.includes('--unlz4') || args.includes('-u')) {
    const filePathIndex = args.indexOf('--unlz4') !== -1 
      ? args.indexOf('--unlz4') + 1 
      : args.indexOf('-u') + 1;
    
    if (filePathIndex < args.length) {
      const filePath = args[filePathIndex];
      await downloadAndDecompressWithUnlz4(filePath);
      return;
    } else {
      console.error('Error: --unlz4 requires a file path argument');
      console.log('Usage: npx ts-node src/index.ts --unlz4 <file_path>');
      return;
    }
  }
  
  // Check for header command
  if (args.includes('--header') || args.includes('-h')) {
    const filePathIndex = args.indexOf('--header') !== -1 
      ? args.indexOf('--header') + 1 
      : args.indexOf('-h') + 1;
    
    if (filePathIndex < args.length) {
      const filePath = args[filePathIndex];
      const sizeInKB = parseInt(args[filePathIndex + 1]) || 64; // Default to 64KB
      await downloadFileHeader(filePath, sizeInKB);
      return;
    } else {
      console.error('Error: --header requires a file path argument');
      console.log('Usage: npx ts-node src/index.ts --header <file_path> [size_in_KB]');
      return;
    }
  }
  
  // Check for transactions command
  if (args.includes('--transactions') || args.includes('-t')) {
    const filePathIndex = args.indexOf('--transactions') !== -1 
      ? args.indexOf('--transactions') + 1 
      : args.indexOf('-t') + 1;
    
    if (filePathIndex < args.length) {
      const filePath = args[filePathIndex];
      
      // Download and decompress the file
      const s3Client = new S3Client({
        region: 'ap-northeast-1',
      });
      
      const downloadParams = {
        Bucket: 'hl-mainnet-node-data',
        Key: filePath,
        RequestPayer: 'requester' as const
      };
      
      try {
        console.log(`Downloading file: ${filePath}...`);
        const getCommand = new GetObjectCommand(downloadParams);
        const fileResponse = await s3Client.send(getCommand);
        
        if (!fileResponse.Body) {
          console.log('No data in file');
          return;
        }
        
        // Convert the readable stream to a buffer with progress reporting and timeout
        const streamToBuffer = async (stream: Readable): Promise<Buffer> => {
          const chunks: Buffer[] = [];
          let totalBytes = 0;
          let lastReportTime = Date.now();
          
          return new Promise((resolve, reject) => {
            // Set a timeout to prevent hanging indefinitely
            const timeout = setTimeout(() => {
              reject(new Error('Download timed out after 5 minutes'));
            }, 5 * 60 * 1000); // 5 minutes timeout
            
            stream.on('data', (chunk: Buffer) => {
              chunks.push(chunk);
              totalBytes += chunk.length;
              
              // Report progress every 5 seconds
              const now = Date.now();
              if (now - lastReportTime > 5000) {
                console.log(`Download progress: ${(totalBytes / (1024 * 1024)).toFixed(2)} MB`);
                lastReportTime = now;
              }
            });
            
            stream.on('error', (err) => {
              clearTimeout(timeout);
              reject(err);
            });
            
            stream.on('end', () => {
              clearTimeout(timeout);
              console.log(`Download complete: ${(totalBytes / (1024 * 1024)).toFixed(2)} MB total`);
              resolve(Buffer.concat(chunks));
            });
          });
        };
        
        console.log('Starting download, this may take a while for large files...');
        const bodyBuffer = await streamToBuffer(fileResponse.Body as Readable);
        
        if (filePath.endsWith('.lz4')) {
          console.log('File is LZ4 compressed. Decompressing...');
          
          try {
            // Decompress using unlz4 command
            console.log('Decompressing with unlz4...');
            const decompressedBuffer = await decompressLz4(bodyBuffer, 'tx_');
            
            console.log(`Decompressed ${bodyBuffer.length} bytes to ${decompressedBuffer.length} bytes`);
            
            // Parse the JSON data
            try {
              const jsonData = JSON.parse(decompressedBuffer.toString('utf-8'));
              console.log('Successfully parsed decompressed data as JSON');
              
              // Extract transactions
              extractTransactions(jsonData);
            } catch (jsonError) {
              console.error('Error parsing JSON:', jsonError);
            }
          } catch (decompressError) {
            console.error('Error decompressing LZ4 file:', decompressError);
          }
        } else {
          // Try to parse as JSON directly
          try {
            const jsonData = JSON.parse(bodyBuffer.toString('utf-8'));
            console.log('Successfully parsed data as JSON');
            
            // Extract transactions
            extractTransactions(jsonData);
          } catch (jsonError) {
            console.error('Error parsing JSON:', jsonError);
          }
        }
      } catch (error) {
        console.error('Error processing file:', error);
      }
      
      return;
    } else {
      console.error('Error: --transactions requires a file path argument');
      console.log('Usage: npx ts-node src/index.ts --transactions <file_path>');
      return;
    }
  }
  
  // Check for download command
  if (args.includes('--download') || args.includes('-d')) {
    const filePathIndex = args.indexOf('--download') !== -1 
      ? args.indexOf('--download') + 1 
      : args.indexOf('-d') + 1;
    
    if (filePathIndex < args.length) {
      const filePath = args[filePathIndex];
      const outputPath = args[filePathIndex + 1] || undefined;
      await downloadAndDecompress(filePath, outputPath);
      return;
    } else {
      console.error('Error: --download requires a file path argument');
      console.log('Usage: npx ts-node src/index.ts --download <file_path> [output_path]');
      return;
    }
  }
  
  // Check for inspect command
  if (args.includes('--inspect') || args.includes('-i')) {
    const filePathIndex = args.indexOf('--inspect') !== -1 
      ? args.indexOf('--inspect') + 1 
      : args.indexOf('-i') + 1;
    
    if (filePathIndex < args.length) {
      const filePath = args[filePathIndex];
      await inspectFile(filePath);
      return;
    } else {
      console.error('Error: --inspect requires a file path argument');
      console.log('Usage: npx ts-node src/index.ts --inspect <file_path>');
      return;
    }
  }
  
  if (args.includes('--list') || args.includes('-l')) {
    // Just list the files in replica_cmds
    const s3Client = new S3Client({
      region: 'ap-northeast-1', // Updated region based on error message
    });
    
    const listParams = {
      Bucket: 'hl-mainnet-node-data',
      Prefix: 'replica_cmds/',
      MaxKeys: 1000,
      RequestPayer: 'requester' as const
    };
    
    try {
      console.log("Listing files in replica_cmds directory...");
      const command = new ListObjectsV2Command(listParams);
      const data = await s3Client.send(command);
      
      if (!data.Contents || data.Contents.length === 0) {
        console.log('No files found');
        return;
      }
      
      console.log(`Found ${data.Contents.length} files in replica_cmds/`);
      
      // Group files by date directory
      const filesByDate = new Map<string, any[]>();
      
      for (const file of data.Contents) {
        if (!file.Key) continue;
        
        // Extract date from path
        const pathParts = file.Key.split('/');
        if (pathParts.length >= 3) {
          const dateDir = pathParts[1];
          const subDir = pathParts[2];
          const key = `${dateDir}/${subDir}`;
          
          if (!filesByDate.has(key)) {
            filesByDate.set(key, []);
          }
          
          filesByDate.get(key)?.push(file);
        }
      }
      
      // Print summary of files by date
      console.log("\n=== Files by Directory ===");
      for (const [dir, files] of filesByDate.entries()) {
        console.log(`\n${dir}: ${files.length} files`);
        
        // Sort files by LastModified
        const sortedFiles = files.sort((a, b) => 
          (b.LastModified?.getTime() || 0) - (a.LastModified?.getTime() || 0)
        );
        
        // Show the 5 most recent files in each directory
        for (let i = 0; i < Math.min(5, sortedFiles.length); i++) {
          const file = sortedFiles[i];
          console.log(`  - ${file.Key} (${file.Size} bytes, Last modified: ${file.LastModified})`);
        }
      }
    } catch (error) {
      console.error('Error listing files:', error);
    }
  } else if (args.includes('--help')) {
    console.log('Hyperliquid Indexer CLI');
    console.log('\nUsage:');
    console.log('  npx ts-node src/index.ts [options]');
    console.log('\nOptions:');
    console.log('  --list, -l                     List files in the replica_cmds directory');
    console.log('  --inspect, -i <path>           Inspect a specific file from S3');
    console.log('  --transactions, -t <path>      Extract and display transactions from a file');
    console.log('  --download, -d <path> [output] Download and decompress a file');
    console.log('  --header <path> [size_in_KB]   Download and analyze just the header of a file');
    console.log('  --unlz4, -u <path>             Download and decompress using unlz4 command line tool');
    console.log('  --help                         Show this help message');
    console.log('  (no options)                  Follow the network tip');
    console.log('\nExamples:');
    console.log('  npx ts-node src/index.ts --list');
    console.log('  npx ts-node src/index.ts --inspect replica_cmds/2025-01-26T12:18:22Z/20250202/486340000.lz4');
    console.log('  npx ts-node src/index.ts --transactions replica_cmds/2025-01-26T12:18:22Z/20250202/486340000.lz4');
    console.log('  npx ts-node src/index.ts --download replica_cmds/2025-01-26T12:18:22Z/20250202/486340000.lz4 output.json');
    console.log('  npx ts-node src/index.ts --header replica_cmds/2025-01-26T12:18:22Z/20250202/486340000.lz4 128');
    console.log('  npx ts-node src/index.ts --unlz4 replica_cmds/2025-01-26T12:18:22Z/20250202/486340000.lz4');
  } else {
    // Run the network tip follower
    followNetworkTip();
  }
}

// Run the main function
main().catch(console.error);
