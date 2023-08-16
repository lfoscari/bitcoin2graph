package it.unimi.dsi.law;

import com.google.common.collect.Iterators;
import it.unimi.dsi.io.FileLinesMutableStringIterable;
import it.unimi.dsi.io.FileLinesMutableStringIterable.FileLinesIterator;
import it.unimi.dsi.lang.MutableString;
import it.unimi.dsi.logging.ProgressLogger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Iterator;
import java.util.NoSuchElementException;

import static it.unimi.dsi.law.graph.Parameters.logInterval;
import static it.unimi.dsi.law.graph.Parameters.logTimeUnit;

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

	public static byte[] columnBytes(MutableString line, int col) {
		return column(line, col).toString().getBytes();
	}

	public static Iterator<MutableString> readTSVs(Path tsv) {
		return new TSVIterator(new File[]{tsv.toFile()});
	}

	public static Iterator<MutableString> readTSVs(Path tsv, LineFilter filter) {
		return readTSVs(new File[]{tsv.toFile()}, filter);
	}

	public static Iterator<MutableString> readTSVs(File[] files, LineFilter filter) {
		Iterator<MutableString> iterator = new TSVIterator(files);

		if (filter != null) {
			iterator = Iterators.filter(iterator, filter::accept);
		}

		return iterator;
	}

	public interface LineFilter {
		boolean accept(MutableString str);
	}

	static class TSVIterator implements Iterator<MutableString> {
		private final Iterator<File> files;
		private FileLinesIterator iterator;
		private File currentFile;

		public TSVIterator(File[] files) {
			if (files.length == 0) {
				throw new IllegalArgumentException("Files list must be non empty");
			}

			this.files = Arrays.stream(files).iterator();
			this.loadNextFile();
		}

		private void loadNextFile() throws NoSuchElementException {
			this.currentFile = this.files.next();
			this.iterator = new FileLinesMutableStringIterable(this.currentFile.toString()).iterator();
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

		public File currentFile() {
			return this.currentFile;
		}
	}
}
