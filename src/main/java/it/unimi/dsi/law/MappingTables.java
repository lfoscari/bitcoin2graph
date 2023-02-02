package it.unimi.dsi.law;

import it.unimi.dsi.bits.TransformationStrategies;
import it.unimi.dsi.fastutil.io.BinIO;
import it.unimi.dsi.lang.MutableString;
import it.unimi.dsi.logging.ProgressLogger;
import it.unimi.dsi.sux4j.mph.GOVMinimalPerfectHashFunction;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.util.Iterator;

import static it.unimi.dsi.law.Parameters.*;
import static it.unimi.dsi.law.Parameters.addressesMap;

public class MappingTables {
    private final ProgressLogger progress;

    public MappingTables (ProgressLogger progress) {
        this.progress = progress == null ? Utils.getProgressLogger(MappingTables.class, "items") : progress;
    }

    public GOVMinimalPerfectHashFunction<MutableString> buildAddressesMap () throws IOException {
        if (addressesMap.toFile().exists()) {
            this.progress.logger.info("Loading address map from memory");
            try {
                return (GOVMinimalPerfectHashFunction<MutableString>) BinIO.loadObject(addressesMap.toFile());
            } catch (ClassNotFoundException e) {
                throw new RuntimeException(e);
            }
        }

        Iterator<MutableString> addressesIt = Utils.readTSVs(addressesFile.toFile(), new MutableString(), null, null);

        this.progress.start("Computing transactions map");
        GOVMinimalPerfectHashFunction<MutableString> map = this.buildMap(addressesIt);
        BinIO.storeObject(map, addressesMap.toFile());
        this.progress.done();

        return map;
    }

    public GOVMinimalPerfectHashFunction<MutableString> buildTransactionsMap () throws IOException {
        if (transactionsMap.toFile().exists()) {
            this.progress.logger.info("Loading transactions map from memory");
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

        this.progress.start("Computing transactions map");
        GOVMinimalPerfectHashFunction<MutableString> map = this.buildMap(transactionsIt);
        BinIO.storeObject(map, transactionsMap.toFile());
        this.progress.done();

        return map;
    }

    public GOVMinimalPerfectHashFunction<MutableString> buildMap (Iterator<MutableString> it) throws IOException {
        File tempDir = Files.createTempDirectory(resources, "map_temp").toFile();
        tempDir.deleteOnExit();

        GOVMinimalPerfectHashFunction.Builder<MutableString> b = new GOVMinimalPerfectHashFunction.Builder<>();
        b.keys(() -> it);
        b.tempDir(tempDir);
        b.transform(TransformationStrategies.rawIso());
        b.signed(1024);
        return b.build();
    }

    public static void main(String[] args) throws IOException {
        new MappingTables(null).buildTransactionsMap();
    }
}
