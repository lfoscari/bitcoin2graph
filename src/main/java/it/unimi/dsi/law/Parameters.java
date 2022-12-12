package it.unimi.dsi.law;

import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static it.unimi.dsi.law.Parameters.BitcoinColumn.*;

public class Parameters {

	// Paths

	public final static Path resources = Path.of("src/main/resources");
	public final static Path graph = resources.resolve("graph");

	public final static Path basename = graph.resolve("bitcoin");
	public final static Path ids = graph.resolve("bitcoin-ids");

	public final static Path inputsDirectory = resources.resolve("inputs");
	public final static Path parsedInputsDirectory = resources.resolve("inputs").resolve("chunks");
	public final static Path inputTransactionsDirectory = resources.resolve("inputs").resolve("transactions");

	public final static Path outputsDirectory = resources.resolve("outputs");
	public final static Path parsedOutputsDirectory = resources.resolve("outputs").resolve("chunks");
	public final static Path outputTransactionsDirectory = resources.resolve("outputs").resolve("transactions");

	public final static Path filtersDirectory = resources.resolve("filters");

	public final static Path addressesFile = resources.resolve("addresses.tsv");
	public final static Path addressesMapFile = resources.resolve("addresses.map");

	public final static int MAX_TVS_LINES = 100;

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

	// Download
	// Select with columns to keep from the TSV and how many inputs and outputs to download
	// from the inputsUrlsFilename and outputsUrlsFilename urls lists.

	public static final List<Integer> INPUTS_IMPORTANT = List.of(SPENDING_TRANSACTION_HASH, RECIPIENT);
	public static final List<Integer> OUTPUTS_IMPORTANT = List.of(TRANSACTION_HASH, RECIPIENT);

	// Logging

	public final static long logInterval = 10;
	public final static TimeUnit logTimeUnit = TimeUnit.SECONDS;

	/* public final static List<String> CRYPTOCURRENCIES = List.of(
		"bitcoin",
		"bitcoin-cash",
		"dash",
		"dogecoin",
		"ethereum",
		"litecoin",
		"zcash"
	); */
}