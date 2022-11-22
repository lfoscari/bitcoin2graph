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
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;

import static it.unimi.dsi.law.Parameters.CleanedBitcoinColumn.*;

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

	public void findMapping() throws IOException {
		ObjectList<Pair<String, BloomFilter<CharSequence>>> filters = loadFilters();
		File[] inputs = Path.of(Parameters.resources, Parameters.inputsDirectory).toFile().listFiles();

		if (inputs == null) {
			throw new FileNotFoundException("No inputs found!");
		}

		for (File input : inputs) {
			this.searchMapping(input, filters);
		}
	}

	private void searchMapping (File input, ObjectList<Pair<String, BloomFilter<CharSequence>>> filters) throws IOException {
		for (String[] inputLine : Utils.readTSV(input)) {
			String transaction = inputLine[0];

			List<String> outputCandidates = filters.stream()
					.filter(f -> f.right().contains(transaction.getBytes()))
					.map(Pair::left).toList();

			for (String outputCandidate : outputCandidates) {
				List<String> recipients = outputContains(outputCandidate, inputLine);

				if (recipients.isEmpty()) {
					continue;
				}

				if (this.arcs == null) {
					System.out.println(inputLine[TRANSACTION_HASH] + " (" + outputCandidate + "): " + inputLine[RECIPIENT] + " ~> " + recipients);
				} else if (this.addressMap != null) {
					this.addArc(inputLine[TRANSACTION_HASH], recipients);
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
		File output = Path.of(Parameters.resources, Parameters.outputsDirectory, outputName).toFile();

		if (!output.exists()) {
			throw new FileNotFoundException("Couldn't find " + output);
		}

		List<String> recipients = new ArrayList<>();

		for (String[] outputLine : Utils.readTSV(output)) {
			if (inputLine[TRANSACTION_HASH].equals(outputLine[TRANSACTION_HASH])) {
				recipients.add(outputLine[RECIPIENT]);
			}
		}

		return recipients;
	}

	private static ObjectList<Pair<String, BloomFilter<CharSequence>>> loadFilters () throws FileNotFoundException {
		File[] filterFiles = Path.of(Parameters.resources, Parameters.filtersDirectory).toFile().listFiles();

		if (filterFiles == null) {
			throw new FileNotFoundException("Generate the filters first!");
		}

		ObjectList<Pair<String, BloomFilter<CharSequence>>> filters = new ObjectArrayList<>(filterFiles.length);
		for (File filter : filterFiles) {
			try {
				filters.add(Pair.of(filter.getName(), (BloomFilter<CharSequence>) BinIO.loadObject(filter)));
			} catch (IOException | ClassNotFoundException e) {
				throw new RuntimeException(e);
			}
		}

		return filters;
	}
}
