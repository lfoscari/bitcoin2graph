package it.unimi.dsi.law;

import it.unimi.dsi.fastutil.Pair;
import it.unimi.dsi.fastutil.io.BinIO;
import it.unimi.dsi.fastutil.objects.Object2LongFunction;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import it.unimi.dsi.fastutil.objects.ObjectList;
import it.unimi.dsi.logging.ProgressLogger;
import it.unimi.dsi.util.BloomFilter;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;

import static it.unimi.dsi.law.Parameters.BitcoinColumn.*;
import static it.unimi.dsi.law.Parameters.INPUTS_IMPORTANT;
import static it.unimi.dsi.law.Parameters.OUTPUTS_IMPORTANT;

public class FindMapping implements Runnable {
	private final LinkedBlockingQueue<long[]> arcs;
	private final Object2LongFunction<String> addressMap;
	private final ProgressLogger progress;

	public FindMapping (final LinkedBlockingQueue<long[]> arcs, Object2LongFunction<String> addressMap, ProgressLogger progress) {
		this.arcs = arcs;
		this.addressMap = addressMap;
		this.progress = progress;
	}

	public FindMapping () {
		this(null, null, new ProgressLogger());
	}

	public static void main (String[] args) {
		new FindMapping().run();
	}

	public void run () {
		try {
			this.progress.start("Searching mappings...");
			this.findMapping();
			this.progress.done();
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	public void findMapping () throws IOException {
		ObjectList<Pair<String, BloomFilter<CharSequence>>> filters = loadFilters();
		File[] inputs = Parameters.inputsDirectory.toFile().listFiles((d, f) -> f.endsWith("tsv"));

		if (inputs == null) {
			throw new FileNotFoundException("No inputs found!");
		}

		for (File input : inputs) {
			this.searchMapping(input, filters);
		}
	}

	private void searchMapping (File input, ObjectList<Pair<String, BloomFilter<CharSequence>>> filters) throws IOException {
		for (String[] inputLine : Utils.readTSV(input, true)) {
			String transaction = inputLine[INPUTS_IMPORTANT.indexOf(SPENDING_TRANSACTION_HASH)];

			List<String> outputCandidates = filters
					.stream()
					.filter(f -> f.right().contains(transaction.getBytes()))
					.map(Pair::left)
					.toList();

			for (String outputCandidate : outputCandidates) {
				List<String> recipients = outputContains(outputCandidate, inputLine);

				if (recipients.isEmpty()) {
					continue;
				}

				if (this.arcs == null) {
					this.progress.logger.info(
							inputLine[INPUTS_IMPORTANT.indexOf(SPENDING_TRANSACTION_HASH)]
							+ ": " + inputLine[INPUTS_IMPORTANT.indexOf(RECIPIENT)]
							+ " ~> " + recipients
					);
				} else if (this.addressMap != null) {
					this.addArc(inputLine[INPUTS_IMPORTANT.indexOf(SPENDING_TRANSACTION_HASH)], recipients);
				} else {
					throw new NullPointerException("You need to pass a function from addresses to longs");
				}
			}

			this.progress.lightUpdate();
		}
	}

	private void addArc (String inputAddress, final List<String> outputAddresses) {
		try {
			for (String outputAddress : outputAddresses) {
				long sender = this.addressMap.getLong(inputAddress);
				long receiver = this.addressMap.getLong(outputAddress);
				this.arcs.put(new long[] { sender, receiver });
			}
		} catch (InterruptedException e) {
			throw new RuntimeException(e);
		}
	}

	private static List<String> outputContains (String outputName, String[] inputLine) throws IOException {
		File output = Parameters.outputsDirectory.resolve(outputName).toFile();

		if (!output.exists()) {
			throw new FileNotFoundException("Couldn't find " + output);
		}

		List<String> recipients = new ArrayList<>();

		for (String[] outputLine : Utils.readTSV(output, true)) {
			String spendingTransaction = inputLine[INPUTS_IMPORTANT.indexOf(SPENDING_TRANSACTION_HASH)];
			String outputTransaction = outputLine[OUTPUTS_IMPORTANT.indexOf(TRANSACTION_HASH)];

			if (spendingTransaction.equals(outputTransaction)) {
				String outputAddress = outputLine[OUTPUTS_IMPORTANT.indexOf(RECIPIENT)];
				recipients.add(outputAddress);
			}
		}

		return recipients;
	}

	private static ObjectList<Pair<String, BloomFilter<CharSequence>>> loadFilters () throws FileNotFoundException {
		File[] filterFiles = Parameters.filtersDirectory.toFile().listFiles();

		if (filterFiles == null) {
			throw new FileNotFoundException("Generate the filters first!");
		}

		ObjectList<Pair<String, BloomFilter<CharSequence>>> filters = new ObjectArrayList<>(filterFiles.length);
		for (File filter : filterFiles) {
			try {
				String filterFilename = filter.getName();
				String outputFilename = filterFilename.substring(0, filterFilename.indexOf(".bloom")) + ".tsv";
				filters.add(Pair.of(outputFilename, (BloomFilter<CharSequence>) BinIO.loadObject(filter)));
			} catch (IOException | ClassNotFoundException e) {
				throw new RuntimeException(e);
			}
		}

		return filters;
	}
}
