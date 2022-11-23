package it.unimi.dsi.law;

import com.opencsv.*;

import java.io.*;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static it.unimi.dsi.law.Parameters.BitcoinColumn.*;

public class TSVCleanup {
	private static final List<Integer> OUTPUTS_IMPORTANT = List.of(TRANSACTION_HASH, INDEX, RECIPIENT);
	private static final List<Integer> INPUTS_IMPORTANT = List.of(SPENDING_TRANSACTION_HASH, INDEX, RECIPIENT);

	public static void main (String[] args) throws IOException {
		Parameters.inputsDirectory.toFile().mkdir();
		Parameters.outputsDirectory.toFile().mkdir();

		File[] toClean = Parameters.originalsDirectory.toFile().listFiles((d, f) -> f.endsWith("tsv"));

		if (toClean == null) {
			throw new FileNotFoundException();
		}

		for (File f : toClean) {
			if (f.toString().contains("input")) {
				clean(f, Parameters.inputsDirectory, INPUTS_IMPORTANT);
			} else {
				clean(f, Parameters.outputsDirectory, OUTPUTS_IMPORTANT);
			}

			// f.delete();
		}
	}

	public static void clean (File tsv, Path destinationDirectory, List<Integer> important) throws IOException {
		Path destinationPath = destinationDirectory.resolve(tsv.getName());

		FileReader originalReader = new FileReader(tsv);
		CSVParser tsvParser = new CSVParserBuilder().withSeparator('\t').build();
		CSVReader tsvReader = new CSVReaderBuilder(originalReader).withCSVParser(tsvParser).build();

		List<String[]> content = new ArrayList<>();

		tsvReader.iterator().forEachRemaining(line -> {
			if (!line[IS_FROM_COINBASE].equals("1")) {
				content.add(important.stream().map(i -> line[i]).toList().toArray(new String[0]));
			}
		});

		if (content.size() <= 1) {
			tsvReader.close();
			return;
		}

		FileWriter destinationWriter = new FileWriter(destinationPath.toString());
		CSVWriter tsvWriter = new CSVWriter(destinationWriter, '\t', '"', '\\', "\n");

		tsvWriter.writeAll(content, false);

		tsvWriter.close();
		tsvReader.close();
	}
}
