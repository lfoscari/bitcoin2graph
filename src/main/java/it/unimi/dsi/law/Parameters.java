package it.unimi.dsi.law;

import java.io.File;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;

public class Parameters {

	// Paths

	public final static Path resources = Path.of("/mnt/big/analysis/lfoscari/bitcoin");
	public final static Path artifacts = resources.resolve("artifacts");
	public final static Path graphDir = resources.resolve("graph-labelled");
	public final static Path clusterFile = graphDir.resolve("clusters");
	public final static Path compressedGraphDir = graphDir.resolve("compressed");

	public final static Path addressesFile = artifacts.resolve("addresses.tsv");
	public final static Path addressesMapFile = artifacts.resolve("address.map");
	public final static Path addressesInverseMapFile = artifacts.resolve("addresses.inverse.map");

	public final static Path transactionsFile = artifacts.resolve("transactions.tsv");
	public final static Path transactionsMapFile = artifacts.resolve("transaction.map");
	public final static Path transactionsDirectory = resources.resolve("transactions");

	public final static Path basename = graphDir.resolve("bitcoin-underlying");
	public final static Path ids = graphDir.resolve("bitcoin-underlying.ids");

	public final static Path inputsDirectory = resources.resolve("inputs");
	public final static Path transactionInputsFile = artifacts.resolve("transactions.inputs.table");

	public final static Path outputsDirectory = resources.resolve("outputs");
	public final static Path transactionOutputsFile = artifacts.resolve("transactions.outputs.table");

	// Bitcoin Blockchair schema for both inputs and outputs

	public static class BitcoinColumn {
		public static final int
				BLOCK_ID = 0,
				TRANSACTION_HASH = 1,
				INDEX = 2,
				TIME = 3,
				VALUE = 4,
				VALUE_USD = 5,
				RECIPIENT = 6,
				TYPE = 7,
				SCRIPT_HEX = 8,
				IS_FROM_COINBASE = 9,
				IS_SPENDABLE = 10,
				SPENDING_BLOCK_ID = 11,
				SPENDING_TRANSACTION_HASH = 12,
				SPENDING_INDEX = 13,
				SPENDING_TIME = 14,
				SPENDING_VALUE_USD = 15,
				SPENDING_SEQUENCE = 16,
				SPENDING_SIGNATURE_HEX = 17,
				SPENDING_WITNESS = 18,
				LIFESPAN = 19,
				CDD = 20;
	}

	// Logging

	public final static long logInterval = 1;
	public final static TimeUnit logTimeUnit = TimeUnit.MINUTES;
	public final static int batchSize = 500_000_000;
}
