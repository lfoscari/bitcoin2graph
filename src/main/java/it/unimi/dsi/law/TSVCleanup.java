package it.unimi.dsi.law;

import com.opencsv.*;
import it.unimi.dsi.law.Parameters.BitcoinColumn;

import java.io.*;
import java.nio.file.Path;
import java.util.List;

import static it.unimi.dsi.law.Parameters.BitcoinColumn.*;

public class TSVCleanup {
    private static final List<Integer> OUTPUTS_IMPORTANT = List.of(TRANSACTION_HASH, INDEX, RECIPIENT);
    private static final List<Integer> INPUTS_IMPORTANT = List.of(SPENDING_TRANSACTION_HASH, INDEX, RECIPIENT);

    public static void main(String[] args) throws IOException {
        Path.of(Parameters.resources, "inputs").toFile().mkdir();
        Path.of(Parameters.resources, "outputs").toFile().mkdir();

        File[] toClean = Path.of(Parameters.resources, "originals").toFile().listFiles();

        if (toClean == null) {
            throw new FileNotFoundException();
        }

        for (File f : toClean) {
            if (f.toString().contains("output")) {
                clean(f, "outputs", OUTPUTS_IMPORTANT);
            } else {
                clean(f, "inputs", INPUTS_IMPORTANT);
            }

            // f.delete();
        }
    }

    public static void clean(File tsv, String destinationDirectory, List<Integer> important) throws IOException {
        Path destinationPath = Path.of(Parameters.resources, destinationDirectory, tsv.getName());

        try (FileWriter destinationWriter = new FileWriter(destinationPath.toString());
             CSVWriter tsvWriter = new CSVWriter(destinationWriter, '\t', '"', '\\', "\n")) {

            FileReader originalReader = new FileReader(tsv);
            CSVParser tsvParser = new CSVParserBuilder().withSeparator('\t').build();
            CSVReader tsvReader = new CSVReaderBuilder(originalReader).withCSVParser(tsvParser).build();

            tsvReader.iterator().forEachRemaining(line -> {
                if (line[IS_FROM_COINBASE].equals("1")) {
                    return;
                }

                List<String> newLine = important.stream().map(i -> line[i]).toList();
                tsvWriter.writeNext(newLine.toArray(new String[0]), false);
            });

            tsvReader.close();
        }
    }
}
