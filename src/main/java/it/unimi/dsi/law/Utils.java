package it.unimi.dsi.law;

import it.unimi.dsi.io.FastBufferedReader;
import it.unimi.dsi.lang.MutableString;
import it.unimi.dsi.logging.ProgressLogger;
import org.rocksdb.*;

import java.io.*;
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

	public static byte[] longToBytes(long data) {
		return new byte[] {
				(byte) ((data >> 56) & 0xff),
				(byte) ((data >> 48) & 0xff),
				(byte) ((data >> 40) & 0xff),
				(byte) ((data >> 32) & 0xff),
				(byte) ((data >> 24) & 0xff),
				(byte) ((data >> 16) & 0xff),
				(byte) ((data >> 8) & 0xff),
				(byte) ((data >> 0) & 0xff),
		};
	}

	public static long[] bytesToLongs(byte[] data) {
		int size = data.length / Long.BYTES;
		long[] result = new long[size];
		for (int i = 0; i < size; i += 1) {
			for (int j = i * Long.BYTES; j < (i + 1) * Long.BYTES; j++) {
				result[i] = (result[i] << 8) + (data[j] & 255);
			}
		}
		return result;
	}

	public static RocksDB startDatabase(boolean readonly, Path location) throws RocksDBException {
		RocksDB.loadLibrary();

		try (Options options = new Options()
				.setCreateIfMissing(true)
				.setMergeOperator(new StringAppendOperator())
				.setDbWriteBufferSize(WRITE_BUFFER_SIZE)
				.setMaxTotalWalSize(MAX_TOTAL_WAL_SIZE)
				.setMaxBackgroundJobs(MAX_BACKGROUND_JOBS)
				.setMaxBytesForLevelBase(MAX_BYTES_FOR_LEVEL_BASE)) {

			if (readonly) {
				return RocksDB.openReadOnly(options, location.toString());
			}

			return RocksDB.open(options, location.toString());
		}
	}

	interface LineFilter {
		boolean accept(String[] line);
	}

	interface LineCleaner {
		String[] clean(String[] line);
	}

	static class CleaningIterable implements Iterator<String[]>, Iterable<String[]> {
		private final Iterator<String[]> iterator;
		private final LineCleaner cleaner;

		public CleaningIterable(Iterable<String[]> iterable, LineCleaner cleaner) {
			this.iterator = iterable.iterator();
			this.cleaner = cleaner;
		}

		@Override
		public boolean hasNext () {
			return this.iterator.hasNext();
		}

		@Override
		public String[] next () {
			return this.cleaner.clean(this.iterator.next());
		}

		@Override
		public Iterator<String[]> iterator () {
			return this;
		}
	}

	static class FilteringIterable implements Iterator<String[]>, Iterable<String[]> {
		private final Iterator<String[]> iterator;
		private final LineFilter filter;

		String[] current;
		boolean hasCurrent;

		public FilteringIterable(Iterable<String[]> iterable, LineFilter filter) {
			this.iterator = iterable.iterator();
			this.filter = filter;
		}

		@Override
		public boolean hasNext () {
			while(!this.hasCurrent) {
				if(!this.iterator.hasNext()) {
					return false;
				}

				String[] next = this.iterator.next();

				if(this.filter.accept(next)) {
					this.current = next;
					this.hasCurrent = true;
				}
			}

			return true;
		}

		@Override
		public String[] next () {
			if (!this.hasNext()) {
				throw new NoSuchElementException();
			}

			String[] next = this.current;
			this.current = null;
			this.hasCurrent = false;

			return next;
		}

		@Override
		public Iterator<String[]> iterator () {
			return this;
		}
	}

	static class TSVIterable implements Iterator<String[]>, Iterable<String[]>, Closeable {
		private final boolean skipHeader;

		private final Iterator<File> files;
		private final MutableString candidate = new MutableString();
		private File currentFile;
		private FileReader reader;
		private FastBufferedReader bufferedReader;

		public TSVIterable (File[] files, boolean skipHeader) throws IOException {
			this.skipHeader = skipHeader;
			this.files = Arrays.stream(files).iterator();
			this.nextFile();
		}

		private void nextFile() throws IOException {
			if (this.reader != null) {
				this.reader.close();
			}

			this.currentFile = this.files.next();
			this.reader = new FileReader(this.currentFile);
			this.bufferedReader = new FastBufferedReader(this.reader);

			if (this.skipHeader) {
				this.bufferedReader.readLine(this.candidate);
				this.candidate.delete(0, this.candidate.length());
			}
		}

		public void close () throws IOException {
			this.reader.close();
			this.bufferedReader.close();
			this.currentFile = null;
		}

		@Override
		public Iterator<String[]> iterator() {
			return this;
		}

		@Override
		public boolean hasNext() {
			try {
				if (!this.candidate.isEmpty() || this.bufferedReader.readLine(this.candidate) != null) {
					return true;
				}

				if (!this.files.hasNext()) {
					this.close();
					return false;
				}

				this.nextFile();
				return true;
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
		}

		@Override
		public String[] next() {
			if (!this.hasNext()) {
				throw new NoSuchElementException();
			}

			String[] line = this.candidate.toString().split("\t");
			this.candidate.delete(0, this.candidate.length());
			return line;
		}
	}

	static Iterable<String[]> readTSVs(File tsv) throws IOException {
		return readTSVs(new File[] { tsv }, null, null, true);
	}

	static Iterable<String[]> readTSVs(File[] files, LineFilter filter, LineCleaner cleaner, boolean skipHeader) throws IOException {
		Iterable<String[]> iterable = new TSVIterable(files, skipHeader);

		if (filter != null) {
			iterable = new FilteringIterable(iterable, filter);
		}

		if (cleaner != null) {
			iterable = new CleaningIterable(iterable, cleaner);
		}

		return iterable;
	}
}
