package it.unimi.dsi.law;

import com.opencsv.CSVParser;
import com.opencsv.CSVParserBuilder;
import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.List;

public class Utils {
	static List<String[]> readTSV (File f) throws IOException {
		return readTSV(f, true);
	}

	static List<String[]> readTSV (File f, boolean skipHeader) throws IOException {
		FileReader fr = new FileReader(f);

		CSVParser tsvParser = new CSVParserBuilder().withSeparator('\t').build();
		CSVReader tsvReader = new CSVReaderBuilder(fr).withCSVParser(tsvParser).build();

		if (!skipHeader) {
			tsvReader.readNext();
		}
		return tsvReader.readAll();
	}
}
