package it.unimi.dsi.law;

import java.nio.file.Path;
import java.util.concurrent.TimeUnit;

public class Parameters {

	// Paths

	public final static Path resources = Path.of("src/main/resources");
	public final static Path artifacts = resources.resolve("artifacts");
	public final static Path graph = resources.resolve("graph");

	public final static Path addressesFile = resources.resolve("addresses.tsv");
	public final static Path addressesMap = artifacts.resolve("addresses.map");
	public final static Path addressesInverseMap = artifacts.resolve("addresses.inverse.map");

	public final static Path transactionsDirectory = resources.resolve("transactions");
	public final static Path transactionsMapFile = artifacts.resolve("transactions.map");
	public final static Path transactionsInverseMapFile = artifacts.resolve("transactions.inverse.map");

	public final static Path basename = graph.resolve("bitcoin");
	public final static Path ids = graph.resolve("bitcoin.ids");

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

	public final static long logInterval = 10;
	public final static TimeUnit logTimeUnit = TimeUnit.SECONDS;

	/* public final static long WRITE_BUFFER_SIZE = 64 * SizeUnit.MB;
	public final static long MAX_TOTAL_WAL_SIZE = SizeUnit.GB;
	public final static int MAX_BACKGROUND_JOBS = 5;
	public final static long MAX_BYTES_FOR_LEVEL_BASE = SizeUnit.GB; */
}