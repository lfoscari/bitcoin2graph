package it.unimi.dsi.law;

import it.unimi.dsi.io.FastBufferedReader;
import it.unimi.dsi.lang.MutableString;

import java.io.*;
import java.nio.charset.Charset;
import java.util.*;

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
		return h > 0 ? h : -1 * h;
	}

	interface LineFilter {
		boolean accept(MutableString str);
	}

	/**
	 * Get specified column as a String (keeps the same backing array)
	 */
	public static String column(MutableString line, int col) {
		int start = 0;
		while (col-- > 0) {
			start = line.indexOf('\t', start) + 1;
		}

		return line.subSequence(start, line.indexOf('\t', start)).toString();
	}

	static class FilteringIterator implements Iterator<MutableString>, Iterable<MutableString> {
		private final Iterator<MutableString> iterator;
		private final LineFilter filter;

		MutableString current;
		boolean hasCurrent;

		public FilteringIterator(Iterable<MutableString> iterable, LineFilter filter) {
			this.iterator = iterable.iterator();
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

		@Override
		public Iterator<MutableString> iterator () {
			return this;
		}
	}

	static class TSVIterator implements Iterator<MutableString>, Iterable<MutableString> {
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

		@Override
		public Iterator<MutableString> iterator () {
			return this;
		}
	}

	static Iterable<MutableString> readTSVs(File tsv, MutableString ms, LineFilter filter) throws IOException {
		return readTSVs(new File[] { tsv }, ms, filter);
	}

	static Iterable<MutableString> readTSVs(File[] files, MutableString ms, LineFilter filter) throws IOException {
		Iterable<MutableString> iterator = new TSVIterator(ms, files);

		if (filter != null) {
			iterator = new FilteringIterator(iterator, filter);
		}

		return iterator;
	}
}
