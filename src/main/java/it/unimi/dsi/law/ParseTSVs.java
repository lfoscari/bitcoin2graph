package it.unimi.dsi.law;

import com.opencsv.*;
import it.unimi.dsi.fastutil.Size64;
import it.unimi.dsi.fastutil.io.BinIO;
import it.unimi.dsi.fastutil.objects.Object2LongArrayMap;
import it.unimi.dsi.fastutil.objects.Object2LongFunction;
import it.unimi.dsi.fastutil.objects.Object2LongOpenHashMap;
import it.unimi.dsi.logging.ProgressLogger;
import it.unimi.dsi.util.BloomFilter;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;

import static it.unimi.dsi.law.Parameters.BitcoinColumn.*;
import static it.unimi.dsi.law.Parameters.*;
import static it.unimi.dsi.law.Utils.*;

public class ParseTSVs {
	private final ProgressLogger progress;
	private int tsvLines, chunkDigits;

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
		parsedInputsDirectory.toFile().mkdir();
		parsedOutputsDirectory.toFile().mkdir();

		{
			File[] files = inputsDirectory.toFile().listFiles((d, s) -> s.endsWith("tsv"));
			if (files == null) throw new NoSuchFileException("No inputs found!");
			this.tsvLines = this.avgNumberOfLines(files) * 2;
			this.chunkDigits = (int) (Math.log10(this.tsvLines * files.length) + 1);

			this.progress.start("Parsing input files with " + this.tsvLines + " lines per chunk");
			this.parseTSV(files, parsedInputsDirectory,
					(line) -> true,
					(line) -> this.keepImportant(line, INPUTS_IMPORTANT));
			this.progress.stop();
		}

		{
			File[] files = outputsDirectory.toFile().listFiles((d, s) -> s.endsWith("tsv"));
			if (files == null) throw new NoSuchFileException("No outputs found!");
			this.tsvLines = this.avgNumberOfLines(files) * 2;
			this.chunkDigits = (int) (Math.log10(this.tsvLines * files.length) + 1);

			this.progress.start("Parsing output files with " + this.tsvLines + " lines per chunk");
			this.parseTSV(files, parsedOutputsDirectory,
					(line) -> line[IS_FROM_COINBASE].equals("0"),
					(line) -> this.keepImportant(line, OUTPUTS_IMPORTANT));
			this.progress.done();
		}
	}

	private void parseTSV (File[] tsvs, Path parsedDirectory, LineFilter filter, LineCleaner cleaner) throws IOException {
		TSVDirectoryLineReader transactionLines = new TSVDirectoryLineReader(tsvs, filter, cleaner, this.progress);

		List<String[]> buffer = new ArrayList<>();
		int count = 0;
		boolean stop = false;

		while (!stop) {
			try {
				for (int i = 0; i < this.tsvLines; i++) {
					String[] transactionLine = transactionLines.next();
					buffer.add(transactionLine);
				}
			} catch (NoSuchElementException e) {
				stop = true;
			}

			String filename = StringUtils.leftPad(String.valueOf(count++), this.chunkDigits + 1, '0');
			this.saveTSV(buffer, parsedDirectory.resolve(filename + ".tsv"));

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

	private int avgNumberOfLines (File[] files) throws IOException {
		int avgLineLength = 543 * Byte.SIZE;
		float numberOfLines = 0f;

		for (int i = 0; i < files.length; i++) {
			float approxLines = (float) Files.size(files[i].toPath()) / avgLineLength;
			numberOfLines = ((numberOfLines * i) + approxLines) / (i + 1);
		}

		return (int) numberOfLines;
	}

	public static void main (String[] args) throws IOException {
		new ParseTSVs().download();
	}
}
