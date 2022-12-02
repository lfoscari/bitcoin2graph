package it.unimi.dsi.law;

import com.opencsv.*;
import it.unimi.dsi.fastutil.io.BinIO;
import it.unimi.dsi.fastutil.objects.Object2LongArrayMap;
import it.unimi.dsi.fastutil.objects.Object2LongFunction;
import it.unimi.dsi.fastutil.objects.Object2LongOpenHashMap;
import it.unimi.dsi.logging.ProgressLogger;
import it.unimi.dsi.util.BloomFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;

import static it.unimi.dsi.law.Parameters.BitcoinColumn.*;
import static it.unimi.dsi.law.Parameters.*;
import static it.unimi.dsi.law.Utils.*;

public class ParseTSVs {
	private final ProgressLogger progress;
	Object2LongFunction<String> addressToLong = new Object2LongOpenHashMap<>();
	long count = 0;

	public ParseTSVs () {
		this(null);
	}

	public ParseTSVs (ProgressLogger progress) {
		if (progress == null) {
			Logger logger = LoggerFactory.getLogger(ParseTSVs.class);
			progress = new ProgressLogger(logger, logInterval, logTimeUnit, "tsvs");
		}

		this.progress = progress;
	}

	private void download () throws IOException {
		filtersDirectory.toFile().mkdir();
		parsedInputsDirectory.toFile().mkdir();
		parsedOutputsDirectory.toFile().mkdir();

		this.progress.start("Parsing input files");
		this.parseTSV(inputsDirectory.toFile().listFiles((d, s) -> s.endsWith("tsv")), INPUTS_IMPORTANT, parsedInputsDirectory, false);
		this.progress.stop();

		this.progress.start("Parsing output files");
		this.parseTSV(outputsDirectory.toFile().listFiles((d, s) -> s.endsWith("tsv")), OUTPUTS_IMPORTANT, parsedOutputsDirectory, true);
		this.progress.done();

		this.progress.logger.info("Bloom filters saved in " + filtersDirectory);
		BinIO.storeObject(this.addressToLong, addressLongMap.toFile());
	}

	private void parseTSV (File[] tsvs, List<Integer> importantColumns, Path parsedDirectory, boolean computeBloomFilters) throws IOException {
		TSVDirectoryLineReader transactionLines = new TSVDirectoryLineReader(tsvs,
				(line) -> line[IS_FROM_COINBASE].equals("0"),
				(line) -> this.keepImportant(line, importantColumns),
				this.progress
		);

		List<String[]> buffer = new ArrayList<>();
		int count = 0;
		boolean stop = false;

		while (!stop) {
			try {
				for (int i = 0; i < MAX_TSV_LINES; i++) {
					String[] transactionLine = transactionLines.next();

					String address = transactionLine[OUTPUTS_IMPORTANT.indexOf(RECIPIENT)];
					if (!this.addressToLong.containsKey(address)) {
						this.addressToLong.put(address, this.count++);
					}

					buffer.add(transactionLine);
				}
			} catch (NoSuchElementException e) {
				stop = true;
			}

			String filename = String.format("%05d", count++);
			this.saveTSV(buffer, parsedDirectory.resolve(filename + ".tsv"));

			if (computeBloomFilters) {
				this.saveBloomFilter(buffer, filtersDirectory.resolve(filename + ".bloom"));
			}

			buffer.clear();
		}
	}

	private void saveTSV (List<String[]> buffer, Path destination) {
		try (FileWriter destinationWriter = new FileWriter(destination.toFile());
			 CSVWriter tsvWriter = new CSVWriter(destinationWriter, '\t', '"', '\\', "\n")) {
			tsvWriter.writeAll(buffer, false);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	public String[] keepImportant (String[] line, List<Integer> importantColumns) {
		String[] filteredLine = new String[importantColumns.size()];

		int j = 0;
		for (int i : importantColumns) {
			filteredLine[j++] = line[i];
		}

		return filteredLine;
	}

	private void saveBloomFilter (List<String[]> content, Path outputPath) throws IOException {
		BloomFilter<CharSequence> transactionFilter = BloomFilter.create(MAX_TSV_LINES, BloomFilter.STRING_FUNNEL);
		content.forEach(line -> transactionFilter.add(line[OUTPUTS_IMPORTANT.indexOf(TRANSACTION_HASH)].getBytes()));
		BinIO.storeObject(transactionFilter, filtersDirectory.resolve(outputPath.getFileName()).toFile());
	}

	public static void main (String[] args) throws IOException {
		new ParseTSVs().download();
	}
}
