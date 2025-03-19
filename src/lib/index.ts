import { HyperliquidIndexer, IndexerConfig } from './HyperliquidIndexer';

async function main() {
  // Parse command line arguments
  const args = process.argv.slice(2);
  const networkArg = args.find(arg => arg.startsWith('--network='))?.split('=')[1] || 'mainnet';
  const startBlockArg = args.find(arg => arg.startsWith('--start-block='))?.split('=')[1];
  const endBlockArg = args.find(arg => arg.startsWith('--end-block='))?.split('=')[1];
  const outputDirArg = args.find(arg => arg.startsWith('--output-dir='))?.split('=')[1] || './data';
  const downloadOnlyArg = args.includes('--download-only');
  const followArg = args.includes('--follow');
  const pollIntervalArg = args.find(arg => arg.startsWith('--poll-interval='))?.split('=')[1];

  if (!startBlockArg) {
    console.error('Usage: npm start -- --network=[mainnet|testnet] --start-block=<number> [--end-block=<number>] --output-dir=<path> [--download-only] [--follow] [--poll-interval=<ms>]');
    process.exit(1);
  }

  // In follow mode, end-block is optional (will find latest)
  if (!endBlockArg && !followArg) {
    console.error('Error: --end-block is required unless --follow is specified');
    process.exit(1);
  }

  const config: IndexerConfig = {
    network: (networkArg === 'testnet' ? 'testnet' : 'mainnet') as 'mainnet' | 'testnet',
    startBlock: parseInt(startBlockArg, 10),
    endBlock: endBlockArg ? parseInt(endBlockArg, 10) : 0, // 0 means find latest in follow mode
    outputDir: outputDirArg,
    downloadOnly: downloadOnlyArg,
    follow: followArg,
    pollInterval: pollIntervalArg ? parseInt(pollIntervalArg, 10) : 60000, // Default to 1 minute
  };

  console.log('Starting Hyperliquid Indexer with config:', {
    ...config,
    pollInterval: config.pollInterval ? `${config.pollInterval/1000} seconds` : undefined
  });

  const indexer = new HyperliquidIndexer(config);
  await indexer.run();
}

main().catch(error => {
  console.error('Error running indexer:', error);
  process.exit(1);
});
