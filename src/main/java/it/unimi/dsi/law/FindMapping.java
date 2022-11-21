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

public class FindMapping {
    public static void main(String[] args) throws IOException {
        ObjectList<Pair<String, BloomFilter<CharSequence>>> filters = loadFilters();
        File[] outputs = Path.of(Parameters.resources, "outputs").toFile().listFiles();

        if (outputs == null) {
            throw new FileNotFoundException("No outputs found!");
        }

        for (File output : outputs) {
            System.out.println("Searching in " + output);
            searchMapping(output, filters);
        }
    }

    private static void searchMapping(File output, ObjectList<Pair<String, BloomFilter<CharSequence>>> filters) throws IOException {
        for (String[] outputLine : Utils.readTSV(output)) {
            String transaction = outputLine[0];

            for (Pair<String, BloomFilter<CharSequence>> filter : filters) {
                if (!filter.right().contains(transaction.getBytes())) {
                    continue;
                }

                String[] inputLine = inputContains(filter.left(), outputLine);

                if (inputLine == null) {
                    continue;
                }

                System.out.println(inputLine[2] + " ~> " + outputLine[2]);
            }
        }
    }

    private static String[] inputContains(String inputName, String[] outputLine) throws IOException {
        File input = Path.of(Parameters.resources, "inputs", inputName).toFile();

        if (!input.exists()) {
            throw new FileNotFoundException("Couldn't find " + input);
        }

        for (String[] inputLine : Utils.readTSV(input)) {
            if (inputLine[0].equals(outputLine[0]) && inputLine[0].equals(outputLine[1])) {
                return inputLine;
            }
        }

        return null;
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
