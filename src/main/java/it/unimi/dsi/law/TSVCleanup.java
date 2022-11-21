package it.unimi.dsi.law;

import com.opencsv.*;
import it.unimi.dsi.fastutil.ints.IntArrays;
import it.unimi.dsi.fastutil.objects.ObjectArrays;

import java.io.*;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class TSVCleanup {
    private static final List<String> OUTPUTS_IMPORTANT = List.of("transaction_hash", "index", "recipient");
    private static final List<String> INPUTS_IMPORTANT = List.of("transaction_hash", "index", "recipient");

    public static void main(String[] args) throws IOException {
        Path.of(Parameters.resources, "inputs").toFile().mkdir();
        Path.of(Parameters.resources, "outputs").toFile().mkdir();

        File[] toClean = Path.of(Parameters.resources, "original").toFile().listFiles();

        if (toClean == null) {
            throw new FileNotFoundException();
        }

        for (File f : toClean) {
            if (f.toString().contains("output")) {
                clean(f, "outputs", OUTPUTS_IMPORTANT);
            } else {
                clean(f, "inputs", INPUTS_IMPORTANT);
            }

            f.delete();
        }
    }

    public static void clean(File tsv, String destinationDirectory, List<String> important) throws IOException {
        Path destinationPath = Path.of(Parameters.resources, destinationDirectory, tsv.getName());

        try (FileWriter destinationWriter = new FileWriter(destinationPath.toString());
             CSVWriter tsvWriter = new CSVWriter(destinationWriter, '\t', '"', '\\', "\n")) {

            tsvWriter.writeNext(important.toArray(new String[]{}), false);

            FileReader originalReader = new FileReader(tsv);
            CSVParser tsvParser = new CSVParserBuilder().withSeparator('\t').build();
            CSVReader tsvReader = new CSVReaderBuilder(originalReader).withCSVParser(tsvParser).build();

            List<String> header = Arrays.stream(tsvReader.readNext()).toList();
            int[] importantColumns = important.stream().mapToInt(header::indexOf).toArray();

            tsvReader.iterator().forEachRemaining(line -> {
                List<String> newLine = Arrays.stream(importantColumns).mapToObj(i -> line[i]).toList();
                tsvWriter.writeNext(newLine.toArray(new String[0]), false);
            });

            tsvReader.close();
        }
    }
}
