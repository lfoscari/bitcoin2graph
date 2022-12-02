package it.unimi.dsi.law;

import com.opencsv.*;
import it.unimi.dsi.fastutil.io.BinIO;
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

public class DownloadInputsOutputs {
	private final ProgressLogger progress;

	public DownloadInputsOutputs () {
		this(null);
	}

	public DownloadInputsOutputs (ProgressLogger progress) {
		if (progress == null) {
			Logger logger = LoggerFactory.getLogger(DownloadInputsOutputs.class);
			progress = new ProgressLogger(logger, logInterval, logTimeUnit, "tsvs");
		}

		this.progress = progress;
	}

	private void download (Path inputs, Path outputs) throws IOException {
		filtersDirectory.toFile().mkdir();

		this.progress.start("Parsing input files");
		this.parseTSV(inputs.toFile().listFiles((d, s) -> s.endsWith("tsv")), INPUTS_IMPORTANT, parsedInputsDirectory);
		this.progress.stop();

		this.progress.start("Parsing output files");
		this.parseTSV(outputs.toFile().listFiles((d, s) -> s.endsWith("tsv")), OUTPUTS_IMPORTANT, parsedOutputsDirectory);
		this.progress.done();
	}

	private void parseTSV (File[] tsvs, List<Integer> importantColumns, Path parsedDirectory) throws FileNotFoundException {
		parsedDirectory.toFile().mkdir();

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
					buffer.add(transactionLines.next());
				}
			} catch (NoSuchElementException e) {
				stop = true;
			}

			String filename = String.format("%05d", count++) + ".tsv";
			this.saveTSV(buffer, parsedDirectory.resolve(filename));
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
		content.forEach(line -> transactionFilter.add(line[TRANSACTION_HASH].getBytes()));
		BinIO.storeObject(transactionFilter, filtersDirectory.resolve(outputPath.getFileName()).toFile());
	}

	public static void main (String[] args) throws IOException {
		new DownloadInputsOutputs().download(inputsDirectory, outputsDirectory);
	}
}
