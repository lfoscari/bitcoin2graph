package it.unimi.dsi.law;

import it.unimi.dsi.fastutil.Pair;
import it.unimi.dsi.fastutil.io.BinIO;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import it.unimi.dsi.fastutil.objects.ObjectList;
import it.unimi.dsi.util.BloomFilter;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static it.unimi.dsi.law.Parameters.CleanedBitcoinColumn.*;

public class FindMapping {
    public static void main (String[] args) throws IOException {
        run();
    }

    public static void run() throws IOException {
        ObjectList<Pair<String, BloomFilter<CharSequence>>> filters = loadFilters();
        File[] inputs = Path.of(Parameters.resources, "inputs").toFile().listFiles();

        if (inputs == null) {
            throw new FileNotFoundException("No outputs found!");
        }

        for (File input : inputs) {
            System.out.println("Searching arcs from " + input);
            searchMapping(input, filters);
        }
    }

    private static void searchMapping(File input, ObjectList<Pair<String, BloomFilter<CharSequence>>> filters) throws IOException {
        for (String[] inputLine : Utils.readTSV(input)) {
            String transaction = inputLine[0];

            List<String> outputCandidates = filters.stream()
                    .filter(f -> f.right().contains(transaction.getBytes()))
                    .map(Pair::left).toList();

            for (String outputCandidate: outputCandidates) {
                List<String> recipients = outputContains(outputCandidate, inputLine);

                if (!recipients.isEmpty()) {
                    System.out.println(inputLine[TRANSACTION_HASH] + " (" + outputCandidate + "): " + inputLine[RECIPIENT] + " ~> " + recipients);
                }
            }
        }
    }

    private static List<String> outputContains(String outputName, String[] inputLine) throws IOException {
        File output = Path.of(Parameters.resources, "outputs", outputName).toFile();

        if (!output.exists()) {
            throw new FileNotFoundException("Couldn't find " + output);
        }

        List<String> recipients = new ArrayList<>();

        for (String[] outputLine : Utils.readTSV(output)) {
            if (inputLine[TRANSACTION_HASH].equals(outputLine[TRANSACTION_HASH])) {
                recipients.add(outputLine[RECIPIENT]);
            }
        }

        return recipients;
    }

    private static ObjectList<Pair<String, BloomFilter<CharSequence>>> loadFilters () throws FileNotFoundException {
        File[] filterFiles = Path.of(Parameters.resources, "filters").toFile().listFiles();

        if (filterFiles == null) {
            throw new FileNotFoundException("Generate filters first!");
        }

        ObjectList<Pair<String, BloomFilter<CharSequence>>> filters = new ObjectArrayList<>(filterFiles.length);
        for (File filter : filterFiles) {
            try {
                // remove 'filter_'
                filters.add(Pair.of(filter.getName(), (BloomFilter<CharSequence>) BinIO.loadObject(filter)));
            } catch (IOException | ClassNotFoundException e) {
                throw new RuntimeException(e);
            }
        }

        return filters;
    }
}
