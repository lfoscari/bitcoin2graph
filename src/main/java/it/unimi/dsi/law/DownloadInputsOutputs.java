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
import java.util.NoSuchElementException;
import java.util.stream.Stream;
import java.util.zip.GZIPInputStream;

import static it.unimi.dsi.law.Parameters.BitcoinColumn.*;
import static it.unimi.dsi.law.Parameters.*;
import static it.unimi.dsi.law.Utils.*;

public class DownloadInputsOutputs {
	private final ProgressLogger progress;

	public DownloadInputsOutputs () {
		this(null);
	}

	public DownloadInputsOutputs (ProgressLogger progress) {
		if (progress == null) {
			Logger logger = LoggerFactory.getLogger(DownloadInputsOutputs.class);
			progress = new ProgressLogger(logger, logInterval, logTimeUnit, "sources");
		}

		this.progress = progress;
	}

	public void run (Path rawInputsOutputs) throws IOException {
		String[] raws = rawInputsOutputs.toFile().list();
		this.progress.expectedUpdates = raws != null ? raws.length : -1;

		this.progress.start("Parsing raw inputs and outputs from " + rawInputsOutputs + "...");
		this.download(rawInputsOutputs.toFile());
		this.progress.stop("Bloom filters saved in " + filtersDirectory);
	}

	private void download (File raws) throws IOException {
		inputsDirectory.toFile().mkdir();
		outputsDirectory.toFile().mkdir();
		filtersDirectory.toFile().mkdir();

		// cat *input* | awk -F'\t' '{if ($10 == "0") { print $13, $7 }}' | split -d -l MAX_TSV_LINES
		this.parseTSV(raws.listFiles((d, s) -> s.endsWith("tsv") && s.contains("input")), INPUTS_IMPORTANT, inputsDirectory);

		// cat *output* | awk -F'\t' '{if ($10 == "0") { print $2, $7 }}' | split -d -l MAX_TSV_LINES
		this.parseTSV(raws.listFiles((d, s) -> s.endsWith("tsv") && s.contains("output")), OUTPUTS_IMPORTANT, outputsDirectory);
	}

	private void parseTSV(File[] tsvs, List<Integer> importantColumns, Path directory) throws FileNotFoundException {
		TSVDirectoryLineReader transactionLines = new TSVDirectoryLineReader(tsvs,
				(line) -> line[IS_FROM_COINBASE].equals("0"),
				(line) -> this.keepImportant(line, importantColumns)
		);

		List<String[]> buffer = new ArrayList<>();
		int totalTSVs = 0;
		boolean stop = false;

		while (!stop) {
			try {
				for (int i = 0; i < MAX_TSV_LINES; i++) {
					buffer.add(transactionLines.next());
				}
			} catch (NoSuchElementException e) {
				stop = true;
			}

			String filename = String.format("%05d", totalTSVs++) + ".tsv";
			this.saveTSV(buffer, directory.resolve(filename));
			buffer.clear();
		}
	}

	private void saveTSV(List<String[]> buffer, Path destination) {
		System.out.println("Saving to " + destination);
		try (FileWriter destinationWriter = new FileWriter(destination.toFile());
			 CSVWriter tsvWriter = new CSVWriter(destinationWriter, '\t', '"', '\\', "\n")) {
			tsvWriter.writeAll(buffer, false);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	public String[] keepImportant(String[] line, List<Integer> importantColumns) {
		String[] filteredLine = new String[importantColumns.size()];

		int j = 0;
		for (int i : importantColumns) {
			filteredLine[j++] = line[i];
		}

		return filteredLine;
	}

	private void saveBloomFilter (List<String[]> content, Path outputPath) throws IOException {
		BloomFilter<CharSequence> transactionFilter = BloomFilter.create(MAX_TSV_LINES, BloomFilter.STRING_FUNNEL);
		content.forEach(line -> transactionFilter.add(line[TRANSACTION_HASH].getBytes()));
		BinIO.storeObject(transactionFilter, filtersDirectory.resolve(outputPath.getFileName()).toFile());
	}

	public static void main (String[] args) throws IOException {
		new DownloadInputsOutputs().run(Path.of(args[0]));
	}
}
