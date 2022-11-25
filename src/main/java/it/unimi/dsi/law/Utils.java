package it.unimi.dsi.law;

import com.opencsv.CSVParser;
import com.opencsv.CSVParserBuilder;
import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;

import java.io.*;
import java.util.List;

public class Utils {
	static List<String[]> readTSV (File f, boolean skipHeader) throws IOException {
		try (FileReader fr = new FileReader(f)) {
			return readTSV(fr, skipHeader);
		}
	}

	static List<String[]> readTSV (InputStream is,  boolean skipHeader) throws IOException {
		try (InputStreamReader isr = new InputStreamReader(is)) {
			return readTSV(isr, skipHeader);
		}
	}

	static List<String[]> readTSV (Reader r, boolean skipHeader) throws IOException {
		CSVParser tsvParser = new CSVParserBuilder().withSeparator('\t').build();
		try (CSVReader tsvReader = new CSVReaderBuilder(r)
				.withCSVParser(tsvParser)
				.withSkipLines(skipHeader ? 1 : 0)
				.build()) {
			return tsvReader.readAll();
		}
	}
}
