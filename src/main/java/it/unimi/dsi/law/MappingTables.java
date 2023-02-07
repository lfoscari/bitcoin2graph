package it.unimi.dsi.law;

import it.unimi.dsi.bits.TransformationStrategies;
import it.unimi.dsi.fastutil.io.BinIO;
import it.unimi.dsi.lang.MutableString;
import it.unimi.dsi.logging.ProgressLogger;
import it.unimi.dsi.sux4j.mph.GOVMinimalPerfectHashFunction;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.util.Iterator;

import static it.unimi.dsi.law.Parameters.*;
import static it.unimi.dsi.law.Parameters.addressesMap;

public class MappingTables {
    public static GOVMinimalPerfectHashFunction<MutableString> buildAddressesMap() throws IOException {
        LoggerFactory.getLogger(MappingTables.class).info("Mapping addresses");

        if (addressesMap.toFile().exists()) {
            try {
                return (GOVMinimalPerfectHashFunction<MutableString>) BinIO.loadObject(addressesMap.toFile());
            } catch (ClassNotFoundException e) {
                throw new RuntimeException(e);
            }
        }

        Iterator<MutableString> addressesIt = Utils.readTSVs(addressesFile.toFile(), new MutableString(), null, null);

        GOVMinimalPerfectHashFunction<MutableString> map = buildMap(addressesIt);
        BinIO.storeObject(map, addressesMap.toFile());

        return map;
    }

    public static GOVMinimalPerfectHashFunction<MutableString> buildTransactionsMap() throws IOException {
        LoggerFactory.getLogger(MappingTables.class).info("Mapping transactions");

        if (transactionsMap.toFile().exists()) {
            try {
                return (GOVMinimalPerfectHashFunction<MutableString>) BinIO.loadObject(transactionsMap.toFile());
            } catch (ClassNotFoundException e) {
                throw new RuntimeException(e);
            }
        }

        Utils.LineCleaner cleaner = (line) -> Utils.column(line, 1);
        Utils.LineFilter filter = (line) -> Utils.columnEquals(line, 7, "0");
        File[] sources = transactionsDirectory.toFile().listFiles((d, s) -> s.endsWith(".tsv"));
        if (sources == null) {
            throw new NoSuchFileException("No transactions found in " + transactionsDirectory);
        }
        Iterator<MutableString> transactionsIt = Utils.readTSVs(sources, new MutableString(), filter, cleaner);

        GOVMinimalPerfectHashFunction<MutableString> map = buildMap(transactionsIt);
        BinIO.storeObject(map, transactionsMap.toFile());

        return map;
    }

    public static GOVMinimalPerfectHashFunction<MutableString> buildMap(Iterator<MutableString> it) throws IOException {
        File tempDir = Files.createTempDirectory(resources, "map_temp_" + it.hashCode()).toFile();
        tempDir.deleteOnExit();

        GOVMinimalPerfectHashFunction.Builder<MutableString> b = new GOVMinimalPerfectHashFunction.Builder<>();
        b.keys(() -> it);
        b.tempDir(tempDir);
        b.transform(TransformationStrategies.iso());
        return b.build();
    }

    public static void main(String[] args) throws IOException {
        MappingTables.buildAddressesMap();
        MappingTables.buildTransactionsMap();
    }
}
