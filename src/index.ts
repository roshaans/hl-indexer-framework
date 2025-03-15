import { HyperliquidIndexer, IndexerConfig } from './HyperliquidIndexer';

async function main() {
  // Parse command line arguments
  const args = process.argv.slice(2);
  const networkArg = args.find(arg => arg.startsWith('--network='))?.split('=')[1] || 'mainnet';
  const startBlockArg = args.find(arg => arg.startsWith('--start-block='))?.split('=')[1];
  const endBlockArg = args.find(arg => arg.startsWith('--end-block='))?.split('=')[1];
  const outputDirArg = args.find(arg => arg.startsWith('--output-dir='))?.split('=')[1] || './data';
  const downloadOnlyArg = args.includes('--download-only');

  if (!startBlockArg || !endBlockArg) {
    console.error('Usage: npm start -- --network=[mainnet|testnet] --start-block=<number> --end-block=<number> --output-dir=<path> [--download-only]');
    process.exit(1);
  }

  const config: IndexerConfig = {
    network: (networkArg === 'testnet' ? 'testnet' : 'mainnet') as 'mainnet' | 'testnet',
    startBlock: parseInt(startBlockArg, 10),
    endBlock: parseInt(endBlockArg, 10),
    outputDir: outputDirArg,
    downloadOnly: downloadOnlyArg,
  };

  console.log('Starting Hyperliquid Indexer with config:', config);

  const indexer = new HyperliquidIndexer(config);
  await indexer.run();
}

main().catch(error => {
  console.error('Error running indexer:', error);
  process.exit(1);
});
