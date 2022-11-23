package it.unimi.dsi.law;

import java.nio.file.Path;
import java.util.concurrent.TimeUnit;

public class Parameters {
	public final static Path resources = Path.of("src/main/resources/");
	public final static Path graph = resources.resolve("graph");

	public final static Path basename = graph.resolve("bitcoin");
	public final static Path ids = graph.resolve("bitcoin-ids");

	public final static Path inputsUrlsFilename = resources.resolve("input-urls.txt");
	public final static Path inputsDirectory = resources.resolve("inputs");

	public final static Path outputsUrlsFilename = resources.resolve("output-urls.txt");
	public final static Path outputsDirectory = resources.resolve("outputs");

	public final static Path filtersDirectory = resources.resolve("filters");
	public final static Path originalsDirectory = resources.resolve("originals");

	public final static Path addressesTSV = resources.resolve("addresses.tsv");
	public final static Path addressLongMap = resources.resolve("addresslong.map");

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

	public static class CleanedBitcoinColumn {
		public static final int
				TRANSACTION_HASH = 0,
				INDEX = 1,
				RECIPIENT = 2;
	}

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