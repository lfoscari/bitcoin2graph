package it.unimi.dsi.law;

import it.unimi.dsi.fastutil.io.BinIO;
import it.unimi.dsi.fastutil.objects.Object2IntFunction;
import it.unimi.dsi.fastutil.objects.Object2LongFunction;
import it.unimi.dsi.logging.ProgressLogger;
import org.rocksdb.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.NoSuchFileException;

import static it.unimi.dsi.law.Parameters.*;
import static it.unimi.dsi.law.Parameters.BitcoinColumn.*;
import static it.unimi.dsi.law.Parameters.addressDatabaseDirectory;
import static it.unimi.dsi.law.RocksDBWrapper.Column.INPUT;
import static it.unimi.dsi.law.RocksDBWrapper.Column.OUTPUT;
import static it.unimi.dsi.law.Utils.*;

public class AddressesDatabase {
    private final ProgressLogger progress;
    private final Object2IntFunction<String> addressMap;
    private final Object2IntFunction<String> transactionMap;

    public AddressesDatabase () {
        this(null, null, null);
    }

    public AddressesDatabase (ProgressLogger progress, Object2IntFunction<String> addressMap, Object2IntFunction<String> transactionMap) {
        if (progress == null) {
            Logger logger = LoggerFactory.getLogger(Blockchain2Webgraph.class);
            progress = new ProgressLogger(logger, logInterval, logTimeUnit, "sources");
            progress.displayLocalSpeed = true;
        }

        this.addressMap = addressMap;
        this.transactionMap = transactionMap;
        this.progress = progress;
    }

    void compute () throws IOException, RocksDBException {
        try (RocksDBWrapper database = new RocksDBWrapper(false, addressDatabaseDirectory)) {
            this.progress.start("Building input addresses database");

            LineCleaner cleaner = (line) -> Utils.keepColumns(line, INPUTS_IMPORTANT);
            File[] sources = inputsDirectory.toFile().listFiles((d, s) -> s.endsWith(".tsv"));

            if (sources == null) {
                throw new NoSuchFileException("Download inputs and outputs first");
            }

            for (String[] line : Utils.readTSVs(sources, null, cleaner)) {
                int addressId = this.addressMap.getInt(line[0]);
                int transactionId = this.transactionMap.getInt(line[1]);

                database.add(INPUT, Utils.intToBytes(addressId), Utils.intToBytes(transactionId));
                this.progress.lightUpdate();
            }

            this.progress.stop();
            this.progress.start("Building output addresses database");

            LineFilter filter = (line) -> Utils.equalsAtColumn(line, "0", IS_FROM_COINBASE);
            cleaner = (line) -> Utils.keepColumns(line, OUTPUTS_IMPORTANT);
            sources = outputsDirectory.toFile().listFiles((d, s) -> s.endsWith(".tsv"));

            if (sources == null) {
                throw new NoSuchFileException("Download inputs and outputs first");
            }

            for (String[] line : Utils.readTSVs(sources, filter, cleaner)) {
                int addressId = this.addressMap.getInt(line[1]);
                int transactionId = this.transactionMap.getInt(line[0]);

                database.add(OUTPUT, Utils.intToBytes(addressId), Utils.intToBytes(transactionId));
                this.progress.lightUpdate();
            }

            this.progress.done();
        }
    }

    public static void main (String[] args) throws IOException, ClassNotFoundException, RocksDBException {
        Object2IntFunction<String> addressMap = (Object2IntFunction<String>) BinIO.loadObject(addressMapLocation.toFile());
        Object2IntFunction<String> transactionMap = (Object2IntFunction<String>) BinIO.loadObject(transactionMapLocation.toFile());

        new AddressesDatabase(null, addressMap, transactionMap).compute();
    }
}
