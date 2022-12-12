package it.unimi.dsi.law;

import com.opencsv.CSVParser;
import com.opencsv.CSVParserBuilder;
import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import it.unimi.dsi.fastutil.io.FastBufferedInputStream;
import it.unimi.dsi.logging.ProgressLogger;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

import javax.sound.sampled.Line;
import java.io.*;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.*;

import static it.unimi.dsi.law.Parameters.*;

public class Utils {
	public static String[] keepImportant (String[] line, List<Integer> importantColumns) {
		String[] filteredLine = new String[importantColumns.size()];

		int j = 0;
		for (int i : importantColumns) {
			filteredLine[j++] = line[i];
		}

		return filteredLine;
	}

	interface LineFilter {
		boolean accept(String[] line);
	}

	interface LineCleaner {
		String[] clean(String[] line);
	}

	static class TSVDirectoryLineReader implements Iterator<String[]>, Iterable<String[]> {
		private final LineFilter filter;
		private final LineCleaner cleaner;
		private final boolean skipHeader;

		private final Iterator<File> files;
		private final CSVParser tsvParser;
		private final ProgressLogger progress;
		private FileReader fileReader = null;
		private CSVReader tsvReader = null;

		public TSVDirectoryLineReader(File file) throws IOException {
			this(new File[] { file }, null, null, false, null);
		}

		public TSVDirectoryLineReader(File[] files, LineFilter filter, LineCleaner cleaner, boolean skipHeader, ProgressLogger progress) throws IOException {
			this.tsvParser = new CSVParserBuilder().withSeparator('\t').build();
			this.filter = filter;
			this.cleaner = cleaner;
			this.skipHeader = skipHeader;
			this.progress = progress;

			this.files = Arrays.stream(files).iterator();
			this.nextFile();
		}

		boolean nextFile () throws IOException {
			if (!this.files.hasNext()) {
				this.close();
				return false;
			}

			File f = this.files.next();

			this.close();

			this.fileReader = new FileReader(f);
			this.tsvReader = new CSVReaderBuilder(this.fileReader)
					.withCSVParser(this.tsvParser)
					.withSkipLines(this.skipHeader ? 1 : 0)
					.build();

			if (this.progress != null) {
				this.progress.update();
			}

			return true;
		}

		private void close () throws IOException {
			if (this.fileReader != null) {
				this.fileReader.close();
			}

			if (this.tsvReader != null) {
				this.tsvReader.close();
			}
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
