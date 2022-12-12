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
	private int chunkDigits;
	private final int MAX_TVS_LINES = 100;

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
			this.chunkDigits = (int) (Math.log10(this.MAX_TVS_LINES * files.length) + 1);

			this.progress.start("Parsing input files with " + this.MAX_TVS_LINES + " lines per chunk");
			this.parseTSV(files, parsedInputsDirectory,
					(line) -> true,
					(line) -> Utils.keepImportant(line, INPUTS_IMPORTANT));
			this.progress.stop();
		}

		{
			File[] files = outputsDirectory.toFile().listFiles((d, s) -> s.endsWith("tsv"));
			if (files == null) throw new NoSuchFileException("No outputs found!");
			this.chunkDigits = (int) (Math.log10(this.MAX_TVS_LINES * files.length) + 1);

			this.progress.start("Parsing output files with " + this.MAX_TVS_LINES + " lines per chunk");
			this.parseTSV(files, parsedOutputsDirectory,
					(line) -> line[IS_FROM_COINBASE].equals("0"),
					(line) -> Utils.keepImportant(line, OUTPUTS_IMPORTANT));
			this.progress.done();
		}
	}

	private void parseTSV (File[] tsvs, Path parsedDirectory, LineFilter filter, LineCleaner cleaner) throws IOException {
		TSVDirectoryLineReader transactionLines = new TSVDirectoryLineReader(tsvs, filter, cleaner, true, this.progress);

		List<String[]> buffer = new ArrayList<>();
		int count = 0;
		boolean stop = false;

		while (!stop) {
			try {
				for (int i = 0; i < this.MAX_TVS_LINES; i++) {
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

	public static void main (String[] args) throws IOException {
		new ParseTSVs().download();
	}
}
