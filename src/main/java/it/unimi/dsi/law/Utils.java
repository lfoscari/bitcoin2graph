package it.unimi.dsi.law;

import it.unimi.dsi.bits.TransformationStrategies;
import it.unimi.dsi.fastutil.io.BinIO;
import it.unimi.dsi.io.FastBufferedReader;
import it.unimi.dsi.lang.MutableString;
import it.unimi.dsi.logging.ProgressLogger;
import it.unimi.dsi.sux4j.mph.GOVMinimalPerfectHashFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.util.*;

import static it.unimi.dsi.law.Parameters.*;

public class Utils {
	public static byte[] longToBytes(long data) {
		return new byte[] {
				(byte) ((data >> 56) & 0xff),
				(byte) ((data >> 48) & 0xff),
				(byte) ((data >> 40) & 0xff),
				(byte) ((data >> 32) & 0xff),
				(byte) ((data >> 24) & 0xff),
				(byte) ((data >> 16) & 0xff),
				(byte) ((data >> 8) & 0xff),
				(byte) (data & 0xff)
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

	public static long hashCode(String s) {
		long h = 0;
		for (byte v : s.getBytes(Charset.defaultCharset())) {
			h = 31 * h + (v & 0xff);
		}
		return h;
	}

	public static ProgressLogger getProgressLogger(Class cls, String itemsName) {
		Logger logger = LoggerFactory.getLogger(cls);
		ProgressLogger progress = new ProgressLogger(logger, logInterval, logTimeUnit, itemsName);
		progress.displayLocalSpeed = true;
		progress.displayFreeMemory = true;

		return progress;
	}

	public static MutableString column(MutableString line, int col) {
		int start = 0, inc;
		while (col-- > 0) {
			if ((inc = line.indexOf('\t', start)) > 0) {
				start = inc + 1;
			} else {
				return line.length(0);
			}
		}

		int end = line.indexOf('\t', start);

		if (end == -1) {
			end = line.length();
		}

		line.delete(end, line.length());
		line.delete(0, start);

		return line;
	}

	public static boolean columnEquals(MutableString line, int col, String other) {
		int start = 0, inc;
		while (col-- > 0) {
			if ((inc = line.indexOf('\t', start)) > 0) {
				start = inc + 1;
			} else {
				return false;
			}
		}

		int end = line.indexOf('\t', start);

		if (end == -1) {
			end = line.length();
		}

		return line.subSequence(start, end).equals(other);
	}

	interface LineFilter {
		boolean accept(MutableString str);
	}

	interface LineCleaner {
		MutableString clean(MutableString str);
	}

	static class CleaningIterator implements Iterator<MutableString> {
		private final Iterator<MutableString> iterator;
		private final LineCleaner cleaner;

		public CleaningIterator(Iterator<MutableString> iterator, LineCleaner cleaner) {
			this.iterator = iterator;
			this.cleaner = cleaner;
		}

		@Override
		public boolean hasNext() {
			return this.iterator.hasNext();
		}

		@Override
		public MutableString next() {
			return this.cleaner.clean(this.iterator.next());
		}
	}

	static class FilteringIterator implements Iterator<MutableString> {
		private final Iterator<MutableString> iterator;
		private final LineFilter filter;

		MutableString current;
		boolean hasCurrent;

		public FilteringIterator(Iterator<MutableString> iterator, LineFilter filter) {
			this.iterator = iterator;
			this.filter = filter;
		}

		@Override
		public boolean hasNext () {
			while(!this.hasCurrent) {
				if(!this.iterator.hasNext()) {
					return false;
				}

				this.current = this.iterator.next();

				if(this.filter.accept(this.current)) {
					this.hasCurrent = true;
				}
			}

			return true;
		}

		@Override
		public MutableString next () {
			if (!this.hasNext()) {
				throw new NoSuchElementException();
			}

			this.hasCurrent = false;
			return this.current;
		}
	}

	static class TSVIterator implements Iterator<MutableString> {
		private final Iterator<File> files;
		private final MutableString candidate;
		private boolean fresh = false;

		private File currentFile;
		private FileReader reader;
		private FastBufferedReader bufferedReader;

		public TSVIterator (MutableString candidate, File[] files) throws IOException {
			this.files = Arrays.stream(files).iterator();
			this.candidate = candidate;
			this.nextFile();
		}

		private void nextFile() throws IOException {
			if (this.reader != null) {
				this.reader.close();
			}

			this.currentFile = this.files.next();
			this.reader = new FileReader(this.currentFile);
			this.bufferedReader = new FastBufferedReader(this.reader);

			// Skip header
			this.bufferedReader.readLine(this.candidate);
			this.fresh = false;
		}

		public void close () throws IOException {
			this.reader.close();
			this.bufferedReader.close();
			this.currentFile = null;
		}

		@Override
		public boolean hasNext() {
			try {
				while (true) {
					if (this.fresh || this.bufferedReader.readLine(this.candidate) != null) {
						this.fresh = true;
						return true;
					}

					if (!this.files.hasNext()) {
						this.close();
						return false;
					}

					this.nextFile();
				}
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
		}

		@Override
		public MutableString next() {
			if (!this.hasNext()) {
				throw new NoSuchElementException();
			}

			this.fresh = false;
			return this.candidate;
		}
	}

	static Iterator<MutableString> readTSVs(File tsv, MutableString ms, LineFilter filter, LineCleaner cleaner) throws IOException {
		return readTSVs(new File[] { tsv }, ms, filter, cleaner);
	}

	static Iterator<MutableString> readTSVs(File[] files, MutableString ms, LineFilter filter, LineCleaner cleaner) throws IOException {
		Iterator<MutableString> iterator = new TSVIterator(ms, files);

		if (filter != null) {
			iterator = new FilteringIterator(iterator, filter);
		}

		if (cleaner != null) {
			iterator = new CleaningIterator(iterator, cleaner);
		}

		return iterator;
	}
}
