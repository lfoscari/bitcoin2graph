package it.unimi.dsi.law;

import com.google.common.collect.Lists;
import com.opencsv.*;
import it.unimi.dsi.fastutil.io.BinIO;
import it.unimi.dsi.fastutil.objects.Object2LongArrayMap;
import it.unimi.dsi.fastutil.objects.Object2LongFunction;
import it.unimi.dsi.logging.ProgressLogger;
import it.unimi.dsi.util.BloomFilter;
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
	private final Object2LongFunction<String> addressLong;
	private long count = 0;

	private final boolean[] inputMask;
	private final boolean[] outputMask;

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
		this.addressLong = new Object2LongArrayMap<>();

		this.inputMask = new boolean[bitcoinColumnsTotal];
		this.outputMask = new boolean[bitcoinColumnsTotal];
		for (int i = 0; i < this.inputMask.length; i++) {
			this.inputMask[i] = INPUTS_IMPORTANT.contains(i);
			this.outputMask[i] = OUTPUTS_IMPORTANT.contains(i);
		}
	}

	public void run () throws IOException {
		this.progress.expectedUpdates = INPUTS_AMOUNT + OUTPUTS_AMOUNT;

		this.progress.start("Downloading urls from " + inputsUrlsFilename + " and " + outputsUrlsFilename);
		this.download(inputsUrlsFilename.toFile(), INPUTS_AMOUNT, false);
		this.download(outputsUrlsFilename.toFile(), OUTPUTS_AMOUNT, true);
		this.progress.stop("Bloom filters saved in " + filtersDirectory);

		this.progress.start("Saving address map...");
		this.saveAddressMap();
		this.progress.stop("Address map saved in " + addressLongMap);
	}

	public void run (Path rawInputsOutputs) throws IOException {
		String[] raws = rawInputsOutputs.toFile().list();
		this.progress.expectedUpdates = raws != null ? raws.length : -1;

		this.progress.start("Parsing raw inputs and outputs from " + rawInputsOutputs + "...");
		this.download(rawInputsOutputs.toFile());
		this.progress.stop("Bloom filters saved in " + filtersDirectory);

		this.progress.start("Saving address map...");
		this.saveAddressMap();
		this.progress.stop("Address map saved in " + addressLongMap);
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
			List<String[]> content = Utils.readTSV(raw, false);
			this.parseTSV(content, raw.getName());
			this.progress.update();
		}

		this.flush();
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
					List<String[]> tsv = Utils.readTSV(gzip, false);
					this.parseTSV(tsv, filename);
					this.progress.update();
				}
			}
		}

		this.flush();
	}

	public void parseTSV (List<String[]> tsv, String filename) throws IOException {
		if (filename.contains("input")) {
			this.parseInputTSV(tsv);
		} else {
			this.parseOutputTSV(tsv);
		}
	}

	private void parseInputTSV(List<String[]> tsv) throws IOException {
		List<String[]> filtered = this.filterTSV(tsv);

		if (this.inputBuffer.size() > MINIMUM_FILTER_ELEMENTS_LINES) {
			String filename = String.format("%05d", this.savedInputs);
			List<String[]> first = this.inputBuffer.subList(0, MINIMUM_FILTER_ELEMENTS_LINES);
			this.saveTSV(first, this.inputMask, INPUTS_IMPORTANT.size(), inputsDirectory.resolve(filename + ".tsv"));

			this.inputBuffer = this.inputBuffer.subList(MINIMUM_FILTER_ELEMENTS_LINES, this.inputBuffer.size());
			this.savedInputs++;
		}

		this.inputBuffer.addAll(filtered);
		this.saveAddresses(filtered);
	}

	private void parseOutputTSV(List<String[]> tsv) throws IOException {
		List<String[]> filtered = this.filterTSV(tsv);

		if (this.outputBuffer.size() > MINIMUM_FILTER_ELEMENTS_LINES) {
			String filename = String.format("%05d", this.savedOutputs);
			List<String[]> first = this.outputBuffer.subList(0, MINIMUM_FILTER_ELEMENTS_LINES);
			this.saveTSV(first, this.outputMask, OUTPUTS_IMPORTANT.size(), outputsDirectory.resolve(filename + ".tsv"));
			this.saveBloomFilter(first, filtersDirectory.resolve(filename + ".bloom"));

			this.outputBuffer = this.outputBuffer.subList(MINIMUM_FILTER_ELEMENTS_LINES, this.outputBuffer.size());
			this.savedOutputs++;
		}

		this.outputBuffer.addAll(filtered);
		this.saveAddresses(filtered);
	}

	private List<String[]> filterTSV (List<String[]> tsv) {
		return tsv.stream().filter(l -> l[IS_FROM_COINBASE].equals("0")).toList();
	}

	private void saveTSV (List<String[]> content, boolean[] columnMask, int size, Path destinationPath) throws IOException {
		try (FileWriter destinationWriter = new FileWriter(destinationPath.toFile());
			 CSVWriter tsvWriter = new CSVWriter(destinationWriter, '\t', '"', '\\', "\n")) {

			String[] filteredLine = new String[size];
			for (String[] line : content) {
				int j = 0;
				for (int i = 0; i < line.length; i++) {
					if (columnMask[i]) {
						filteredLine[j++] = line[i];
					}
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

	private void saveAddresses (List<String[]> content) {
		for (String[] line : content) {
			String address = line[RECIPIENT];
			if (this.addressLong.containsKey(address)) {
				this.addressLong.put(address, this.count++);
			}
		}
	}

	private void saveAddressMap () throws IOException {
		BinIO.storeObject(this.addressLong, addressLongMap.toFile());
	}

	private void flush () throws IOException {
		this.saveTSV(this.inputBuffer, this.inputMask, INPUTS_IMPORTANT.size(), inputsDirectory.resolve(String.format("%05d.tsv", this.savedInputs)));
		this.saveTSV(this.outputBuffer, this.outputMask, OUTPUTS_IMPORTANT.size(), outputsDirectory.resolve(String.format("%05d.tsv", this.savedOutputs)));
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
