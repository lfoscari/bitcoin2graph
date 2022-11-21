package it.unimi.dsi.law;

import com.google.common.hash.Funnels;
import com.opencsv.CSVParser;
import com.opencsv.CSVParserBuilder;
import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import it.unimi.dsi.fastutil.io.BinIO;
import it.unimi.dsi.util.BloomFilter;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;

public class TransactionBloom {

    // Magari mettere anche gli indici?

    public static void main(String[] args) throws IOException {
        final Path filtersDirectory = Path.of(Parameters.resources, "filters");
        filtersDirectory.toFile().mkdir();
        File[] inputs = Path.of(Parameters.resources, "inputs").toFile().listFiles((d, f) -> f.endsWith("tsv"));

        if (inputs == null) {
            throw new FileNotFoundException("No inputs found!");
        }

        for (File input : inputs) {
            save(input);
        }

        System.out.println("Filters saved in " + filtersDirectory);
    }

    public static void save(File input) throws IOException {
        BloomFilter<CharSequence> transactionFilter = BloomFilter.create(1000, Funnels.stringFunnel(StandardCharsets.UTF_8));

        FileReader originalReader = new FileReader(input);
        CSVParser tsvParser = new CSVParserBuilder().withSeparator('\t').build();
        CSVReader tsvReader = new CSVReaderBuilder(originalReader).withCSVParser(tsvParser).build();

        tsvReader.readNext(); // header
        tsvReader.iterator().forEachRemaining(line -> transactionFilter.add(line[0].getBytes()));

        BinIO.storeObject(transactionFilter, Path.of(Parameters.resources, "filters", input.getName()).toFile());
    }
}
