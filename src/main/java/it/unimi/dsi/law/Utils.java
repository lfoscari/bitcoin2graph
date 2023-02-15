package it.unimi.dsi.law;

import com.google.common.collect.Iterators;
import it.unimi.dsi.fastutil.BigArrays;
import it.unimi.dsi.fastutil.io.BinIO;
import it.unimi.dsi.fastutil.objects.ObjectBigArrays;
import it.unimi.dsi.io.FileLinesMutableStringIterable;
import it.unimi.dsi.io.FileLinesMutableStringIterable.FileLinesIterator;
import it.unimi.dsi.lang.MutableString;
import it.unimi.dsi.logging.ProgressLogger;
import it.unimi.dsi.sux4j.mph.GOVMinimalPerfectHashFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.file.Path;
import java.util.*;

import static it.unimi.dsi.law.Parameters.*;

public class Utils {
	public static ProgressLogger getProgressLogger(Class cls, String itemsName) {
		Logger logger = LoggerFactory.getLogger(cls);
		ProgressLogger progress = new ProgressLogger(logger, logInterval, logTimeUnit, itemsName);
		progress.displayFreeMemory = true;

		return progress;
	}

	public static CharSequence column(MutableString line, int col) {
		int start = 0, inc;
		while (col-- > 0) {
			if ((inc = line.indexOf('\t', start)) > 0) {
				start = inc + 1;
			} else {
				throw new RuntimeException("Column number too high");
			}
		}

		int end = line.indexOf('\t', start);

		if (end == -1) {
			end = line.length();
		}

		return line.subSequence(start, end);
	}

	interface LineFilter {
		boolean accept(MutableString str);
	}

	static class TSVIterator implements Iterator<MutableString> {
		private final Iterator<File> files;
		private FileLinesIterator iterator;

		public TSVIterator (File[] files) {
			if (files.length == 0) {
				throw new IllegalArgumentException("Files list must be non empty");
			}

			this.files = Arrays.stream(files).iterator();
			this.loadNextFile();
		}

		private void loadNextFile() throws NoSuchElementException {
			String filename = this.files.next().toString();
			this.iterator = new FileLinesMutableStringIterable(filename).iterator();
			this.iterator.next(); // skip header
		}

		@Override
		public boolean hasNext() {
			return this.iterator.hasNext() || this.files.hasNext();
		}

		@Override
		public MutableString next() {
			if (!this.hasNext()) {
				throw new NoSuchElementException();
			}

			while (!this.iterator.hasNext()) {
				this.loadNextFile();
			}

			return this.iterator.next();
		}
	}

	static Iterator<MutableString> readTSVs(Path tsv) {
		return new TSVIterator(new File[] { tsv.toFile() });
	}

	static Iterator<MutableString> readTSVs(File[] files, LineFilter filter) {
		Iterator<MutableString> iterator = new TSVIterator(files);

		if (filter != null) {
			iterator = Iterators.filter(iterator, filter::accept);
		}

		return iterator;
	}
}
