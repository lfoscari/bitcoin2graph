package it.unimi.dsi.law;

import com.opencsv.CSVWriter;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.List;

import static it.unimi.dsi.law.Parameters.BitcoinColumn.TRANSACTION_HASH;

public class AddressesFromOriginals {
	public static void main (String[] args) throws IOException {
		File[] sources = Parameters.originalsDirectory.toFile().listFiles((d, f) -> f.endsWith("tsv"));

		if (sources == null) {
			throw new NoSuchFileException("Download inputs and outputs first");
		}

		FileWriter destinationWriter = new FileWriter(Parameters.addressesTSV.toFile());
		CSVWriter tsvWriter = new CSVWriter(destinationWriter, '\t', '"', '\\', "\n");

		tsvWriter.writeNext(new String[] { "addresses" }, false);

		HashSet<String> addresses = new HashSet<>();

		for (File f : sources) {
			addresses.addAll(Utils.readTSV(f).stream().map(line -> line[TRANSACTION_HASH]).toList());
		}

		tsvWriter.writeAll(addresses.stream().map(a -> new String[] { a }).toList(), false);
		tsvWriter.close();
		destinationWriter.close();
	}
}
