package it.unimi.dsi.law;

import it.unimi.dsi.fastutil.bytes.ByteArrays;
import it.unimi.dsi.fastutil.ints.IntArrays;
import it.unimi.dsi.io.FastBufferedReader;
import it.unimi.dsi.lang.MutableString;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.*;

public class Utils {
	public static byte[] intToBytes(final int data) {
		ByteBuffer bb = ByteBuffer.allocate(Integer.BYTES);
		bb.putInt(data);
		return bb.array();
	}

	public static long[] bytesToLongs(final byte[] data) {
		ByteBuffer bb = ByteBuffer.wrap(data);

		int size = data.length / Long.BYTES;
		long[] result = new long[size];

		for (int i = 0; i < size; i++) {
			result[i] = bb.getInt();
		}

		return result;
	}

	public static int[] distinct(final int[] data) {
		int max = data.length;
		int i = 0;
		while (i < max) {
			boolean unique = true;

			for (int j = i + 1; j < max; j++) {
				if (data[i] == data[j]) {
					IntArrays.swap(data, j, max - 1);
					unique = false;
					break;
				}
			}

			if (unique) {
				i++;
			} else {
				max--;
			}
		}

		return IntArrays.trim(data, max);
	}

	public static int[] bytesToInts(final byte[] data) {
		ByteBuffer bb = ByteBuffer.wrap(data);

		int size = data.length / Integer.BYTES;
		int[] result = new int[size];

		for (int i = 0; i < size; i++) {
			result[i] = bb.getInt();
		}

		return result;
	}

	public static int[] intersect(final int[] a, final int[] b) {
		int max = a.length;
		int i = 0;

		while (i < max) {
			boolean exists = false;
			for (int l : b) {
				if (a[i] == l) {
					exists = true;
					break;
				}
			}

			if (!exists) {
				IntArrays.swap(a, i, max - 1);
				max--;
			} else {
				i++;
			}
		}

		return IntArrays.trim(a, max);
	}

	interface LineFilter {
		boolean accept(MutableString str);
	}

	interface LineCleaner {
		MutableString clean(MutableString str);
	}

	/**
	 * From a tab-separated mutable string delete everything but the given columns,
	 * which must the sorted.
	 */
	public static MutableString keepColumns (MutableString line, List<Integer> sortedColumns) {
		int previousColumn = 0;
		int baseCursor = -1;

		for (int important : sortedColumns) {
			int cursor = baseCursor;
			important -= previousColumn;

			previousColumn += important;

			while (important-- > 0) {
				cursor = line.indexOf('\t', cursor) + 1;
			}

			int nextCursor = line.indexOf('\t', cursor);

			if (nextCursor == -1) {
				nextCursor = line.length();
			}

			line.delete(baseCursor + 1, Integer.max(cursor, 0));
			baseCursor += nextCursor - (cursor - 1);
		}

		line.delete(baseCursor, line.length());
		return line;
	}

	/**
	 * Compares the given string with the value in the given column in a tab-separated mutable string.
	 */
	public static boolean equalsAtColumn(MutableString line, String comparison, int column) {
		int start = 0;
		while (column-- > 0) {
			start = line.indexOf('\t', start) + 1;
		}

		return line.subSequence(start, line.indexOf('\t', start)).equals(comparison);
	}

	static class CleaningIterable implements Iterator<MutableString>, Iterable<MutableString> {
		private final Iterator<MutableString> iterator;
		private final LineCleaner cleaner;

		public CleaningIterable(Iterable<MutableString> iterable, LineCleaner cleaner) {
			this.iterator = iterable.iterator();
			this.cleaner = cleaner;
		}

		@Override
		public boolean hasNext () {
			return this.iterator.hasNext();
		}

		@Override
		public MutableString next () {
			return this.cleaner.clean(this.iterator.next());
		}

		@Override
		public Iterator<MutableString> iterator () {
			return this;
		}
	}

	static class SplittingIterable implements Iterator<String[]>, Iterable<String[]> {
		private final Iterator<MutableString> iterator;

		public SplittingIterable(Iterable<MutableString> iterable) {
			this.iterator = iterable.iterator();
		}

		@Override
		public boolean hasNext () {
			return this.iterator.hasNext();
		}

		@Override
		public String[] next () {
			return this.iterator.next().toString().split("\t");
		}

		@Override
		public Iterator<String[]> iterator () {
			return this;
		}
	}

	static class FilteringIterable implements Iterator<MutableString>, Iterable<MutableString> {
		private final Iterator<MutableString> iterator;
		private final LineFilter filter;

		MutableString current;
		boolean hasCurrent;

		public FilteringIterable(Iterable<MutableString> iterable, LineFilter filter) {
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

	static class TSVIterable implements Iterator<MutableString>, Iterable<MutableString>, Closeable {
		private final Iterator<File> files;
		private final MutableString candidate;
		private boolean fresh = false;

		private File currentFile;
		private FileReader reader;
		private FastBufferedReader bufferedReader;

		public TSVIterable (MutableString candidate, File[] files) throws IOException {
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
		}

		public void close () throws IOException {
			this.reader.close();
			this.bufferedReader.close();
			this.currentFile = null;
		}

		@Override
		public Iterator<MutableString> iterator() {
			return this;
		}

		@Override
		public boolean hasNext() {
			try {
				if (this.fresh || this.bufferedReader.readLine(this.candidate) != null) {
					this.fresh = true;
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
		public MutableString next() {
			if (!this.hasNext()) {
				throw new NoSuchElementException();
			}

			this.fresh = false;
			return this.candidate;
		}
	}

	static Iterable<String[]> readTSVs(File tsv) throws IOException {
		return readTSVs(new File[] { tsv }, null, null);
	}

	static Iterable<String[]> readTSVs(File[] files, LineFilter filter, LineCleaner cleaner) throws IOException {
		Iterable<MutableString> iterable = new TSVIterable(new MutableString(), files);

		if (filter != null) {
			iterable = new FilteringIterable(iterable, filter);
		}

		if (cleaner != null) {
			iterable = new CleaningIterable(iterable, cleaner);
		}

		return new SplittingIterable(iterable);
	}
}
