package it.unimi.dsi.law;

import com.google.common.collect.Lists;
import com.opencsv.*;
import it.unimi.dsi.fastutil.ints.Int2LongFunction;
import it.unimi.dsi.fastutil.ints.Int2LongOpenCustomHashMap;
import it.unimi.dsi.fastutil.ints.Int2LongOpenHashMap;
import it.unimi.dsi.fastutil.io.BinIO;
import it.unimi.dsi.fastutil.objects.Object2LongArrayMap;
import it.unimi.dsi.fastutil.objects.Object2LongFunction;
import it.unimi.dsi.fastutil.objects.Object2LongOpenHashMap;
import it.unimi.dsi.logging.ProgressLogger;
import it.unimi.dsi.util.BloomFilter;
import it.unimi.dsi.util.StringMaps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.URL;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;
import java.util.zip.GZIPInputStream;

import static it.unimi.dsi.law.Parameters.BitcoinColumn.*;
import static it.unimi.dsi.law.Parameters.*;

public class DownloadInputsOutputs {
	private final ProgressLogger progress;

	private List<String[]> inputBuffer;
	private long savedInputs = 1;

	private List<String[]> outputBuffer;
	private long savedOutputs = 1;

	public DownloadInputsOutputs () {
		this(null);
	}

	public DownloadInputsOutputs (ProgressLogger progress) {
		if (progress == null) {
			Logger logger = LoggerFactory.getLogger(DownloadInputsOutputs.class);
			progress = new ProgressLogger(logger, logInterval, logTimeUnit, "sources");
		}

		this.progress = progress;
		this.inputBuffer = new ArrayList<>();
		this.outputBuffer = new ArrayList<>();
	}

	public void run () throws IOException {
		this.progress.expectedUpdates = INPUTS_AMOUNT + OUTPUTS_AMOUNT;

		this.progress.start("Downloading urls from " + inputsUrlsFilename + " and " + outputsUrlsFilename);
		this.download(inputsUrlsFilename.toFile(), INPUTS_AMOUNT, false);
		this.download(outputsUrlsFilename.toFile(), OUTPUTS_AMOUNT, true);
		this.progress.stop("Bloom filters saved in " + filtersDirectory);
	}

	public void run (Path rawInputsOutputs) throws IOException {
		String[] raws = rawInputsOutputs.toFile().list();
		this.progress.expectedUpdates = raws != null ? raws.length : -1;

		this.progress.start("Parsing raw inputs and outputs from " + rawInputsOutputs + "...");
		this.download(rawInputsOutputs.toFile());
		this.progress.stop("Bloom filters saved in " + filtersDirectory);
	}

	private void download (File urls, int limit, boolean computeBloomFilters) throws IOException {
		inputsDirectory.toFile().mkdir();
		outputsDirectory.toFile().mkdir();

		if (computeBloomFilters) {
			filtersDirectory.toFile().mkdir();
		}

		try (FileReader reader = new FileReader(urls)) {
			List<String> toDownload = new BufferedReader(reader).lines().toList();

			if (limit >= 0) {
				toDownload = toDownload.subList(0, limit);
			}

			for (String s : toDownload) {
				URL url = new URL(s);
				String filename = s.substring(s.lastIndexOf("/") + 1, s.indexOf(".gz?"));

				try (GZIPInputStream gzip = new GZIPInputStream(url.openStream())) {
					List<String[]> tsv = Utils.readTSV(gzip, true);
					this.parseTSV(tsv, filename);
					this.progress.update();
				}
			}
		}

		this.flush();
	}

	private void download (File raws) throws IOException {
		inputsDirectory.toFile().mkdir();
		outputsDirectory.toFile().mkdir();
		filtersDirectory.toFile().mkdir();

		File[] rawFiles = raws.listFiles((d, f) -> f.endsWith("tsv"));
		if (rawFiles == null) {
			throw new NoSuchFileException("No outputs or inputs found in " + raws);
		}

		for (File raw : rawFiles) {
			List<String[]> content = Utils.readTSV(raw, true);
			this.parseTSV(content, raw.getName());
			this.progress.update();
		}

		this.flush();
	}

	public void parseTSV (List<String[]> tsv, String filename) throws IOException {
		boolean isInput = filename.contains("input");
		List<String[]> buffer = isInput ? this.inputBuffer : this.outputBuffer;

		tsv.removeIf(l -> l[IS_FROM_COINBASE].equals("1"));
		buffer.addAll(tsv);

		if (buffer.size() > MINIMUM_FILTER_ELEMENTS_LINES) {
			String chunkFilename = String.format("%05d", this.savedInputs);
			Path destinationFile = isInput ?
					inputsDirectory.resolve(chunkFilename + ".tsv") :
					outputsDirectory.resolve(chunkFilename + ".tsv");

			List<String[]> first = buffer.subList(0, MINIMUM_FILTER_ELEMENTS_LINES);
			this.saveTSV(first, isInput ? INPUTS_IMPORTANT : OUTPUTS_IMPORTANT, destinationFile);

			first.clear();
			this.savedInputs++;
		}
	}

	private void saveTSV (List<String[]> content, List<Integer> importantColumns, Path destinationPath) throws IOException {
		try (FileWriter destinationWriter = new FileWriter(destinationPath.toFile());
			 CSVWriter tsvWriter = new CSVWriter(destinationWriter, '\t', '"', '\\', "\n")) {

			String[] filteredLine = new String[importantColumns.size()];
			for (String[] line : content) {
				int j = 0;
				for (int i : importantColumns) {
					filteredLine[j++] = line[i];
				}

				tsvWriter.writeNext(filteredLine, false);
			}
		}
	}

	private void saveBloomFilter (List<String[]> content, Path outputPath) throws IOException {
		BloomFilter<CharSequence> transactionFilter = BloomFilter.create(MINIMUM_FILTER_ELEMENTS_LINES, BloomFilter.STRING_FUNNEL);
		content.forEach(line -> transactionFilter.add(line[TRANSACTION_HASH].getBytes()));
		BinIO.storeObject(transactionFilter, filtersDirectory.resolve(outputPath.getFileName()).toFile());
	}

	private void flush () throws IOException {
		this.saveTSV(this.inputBuffer, INPUTS_IMPORTANT, inputsDirectory.resolve(String.format("%05d.tsv", this.savedInputs)));
		this.saveTSV(this.outputBuffer, OUTPUTS_IMPORTANT, outputsDirectory.resolve(String.format("%05d.tsv", this.savedOutputs)));
		this.saveBloomFilter(this.outputBuffer, outputsDirectory.resolve(String.format("%05d.bloom", this.savedOutputs)));

		this.inputBuffer.clear();
		this.outputBuffer.clear();
		this.savedInputs++;
	}

	public static void main (String[] args) throws IOException {
		if (args.length > 0) {
			new DownloadInputsOutputs().run(Path.of(args[0]));
		} else {
			new DownloadInputsOutputs().run();
		}
	}
}
