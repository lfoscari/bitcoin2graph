package it.unimi.dsi.law;

import com.opencsv.CSVWriter;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.List;

import static it.unimi.dsi.law.Parameters.BitcoinColumn.TRANSACTION_HASH;

public class AddressesFromOriginals {
	public static void main (String[] args) throws IOException {
		File[] sources = Path.of(Parameters.resources, Parameters.originalsDirectory).toFile().listFiles();

		if (sources == null) {
			throw new NoSuchFileException("AAAAAAAAAAAAAAA");
		}

		FileWriter destinationWriter = new FileWriter(Parameters.resources + Parameters.addressesTSV);
		CSVWriter tsvWriter = new CSVWriter(destinationWriter, '\t', '"', '\\', "\n");

		tsvWriter.writeNext(new String[] { "addresses" }, false);

		for (File f : sources) {
			List<String[]> rows = Utils.readTSV(f).stream().map(line -> new String[] { line[TRANSACTION_HASH] }).toList();
			tsvWriter.writeAll(rows, false);

		}

		tsvWriter.close();
		destinationWriter.close();
	}
}
