package it.unimi.dsi.law;

import com.opencsv.CSVParser;
import com.opencsv.CSVParserBuilder;
import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import it.unimi.dsi.fastutil.io.FastBufferedInputStream;
import it.unimi.dsi.logging.ProgressLogger;

import javax.sound.sampled.Line;
import java.io.*;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.*;

public class Utils {
	static List<String[]> readTSV (File f, boolean skipHeader, LineFilter filter) throws IOException {
		try (FileReader fr = new FileReader(f)) {
			return readTSV(fr, skipHeader, filter);
		}
	}

	static List<String[]> readTSV (InputStream is,  boolean skipHeader, LineFilter filter) throws IOException {
		try (InputStreamReader isr = new InputStreamReader(is)) {
			return readTSV(isr, skipHeader, filter);
		}
	}

	interface LineFilter {
		boolean accept(String[] line);
	}

	interface LineCleaner {
		String[] clean(String[] line);
	}

	static List<String[]> readTSV (Reader r, boolean skipHeader, LineFilter filter) throws IOException {
		CSVParser tsvParser = new CSVParserBuilder().withSeparator('\t').build();
		try (CSVReader tsvReader = new CSVReaderBuilder(r)
				.withCSVParser(tsvParser)
				.withSkipLines(skipHeader ? 1 : 0)
				.build()) {

			ArrayList<String[]> lines = new ArrayList<>();
			while (true) {
				String[] line = tsvReader.readNext();

				if (line == null) {
					break;
				}

				if (filter.accept(line)) {
					lines.add(line);
				}
			}

			return lines;
		}
	}

	static class TSVDirectoryLineReader implements Iterator<String[]>, Iterable<String[]> {
		private final LineFilter filter;
		private final LineCleaner cleaner;

		private final Iterator<File> files;
		private final CSVParser tsvParser;
		private final ProgressLogger progress;
		private CSVReader tsvReader;

		public TSVDirectoryLineReader(File file) throws FileNotFoundException {
			this(new File[] { file }, null, null, null);
		}

		public TSVDirectoryLineReader(File[] files, LineFilter filter, LineCleaner cleaner, ProgressLogger progress) throws FileNotFoundException {
			this.tsvParser = new CSVParserBuilder().withSeparator('\t').build();
			this.filter = filter;
			this.cleaner = cleaner;
			this.progress = progress;

			this.files = Arrays.stream(files).iterator();
			this.nextFile();
		}

		boolean nextFile () throws FileNotFoundException {
			if (!this.files.hasNext()) {
				this.tsvReader = null;
				return false;
			}

			File f = this.files.next();

			if (this.progress != null) {
				this.progress.update();
			}

			FileReader r = new FileReader(f);

			this.tsvReader = new CSVReaderBuilder(r)
					.withCSVParser(this.tsvParser)
					.build();

			return true;
		}

		@Override
		public Iterator<String[]> iterator() {
			return this;
		}

		@Override
		public boolean hasNext() {
			return false;
		}

		@Override
		public String[] next() {
			String[] candidate;

			try {
				while (true) {
					candidate = this.tsvReader.readNext();

					if (candidate == null) {
						if (!this.nextFile()) {
							throw new NoSuchElementException();
						}
					} else if (this.filter == null || this.filter.accept(candidate)) {
						return this.cleaner != null ? this.cleaner.clean(candidate) : candidate;
					}
				}
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
		}
	}
}
