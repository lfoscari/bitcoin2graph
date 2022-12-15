package it.unimi.dsi.law;

import it.unimi.dsi.fastutil.ints.Int2LongFunction;
import it.unimi.dsi.fastutil.io.BinIO;
import it.unimi.dsi.logging.ProgressLogger;
import org.rocksdb.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static it.unimi.dsi.law.Parameters.*;
import static it.unimi.dsi.law.Parameters.BitcoinColumn.*;
import static it.unimi.dsi.law.RocksDBWrapper.Column.INPUT;
import static it.unimi.dsi.law.RocksDBWrapper.Column.OUTPUT;
import static it.unimi.dsi.law.Utils.*;

public class TransactionsDatabase {
    // private final Int2LongFunction addressMap;
    // private final Int2LongFunction transactionMap;

    private final ProgressLogger progress;

    public TransactionsDatabase() throws IOException, ClassNotFoundException {
        this(null);
    }

    public TransactionsDatabase(ProgressLogger progress) throws IOException, ClassNotFoundException {
        if (progress == null) {
            Logger logger = LoggerFactory.getLogger(Blockchain2Webgraph.class);
            progress = new ProgressLogger(logger, logInterval, logTimeUnit, "sources");
            progress.displayLocalSpeed = true;
        }

        this.progress = progress;
        // this.addressMap = (Int2LongFunction) Utils.loadFullObject(addressesMapFile.toFile());
        // this.transactionMap = (Int2LongFunction) Utils.loadFullObject(transactionsMapFile.toFile());
    }

    void compute() throws IOException, RocksDBException {
        try (RocksDBWrapper database = new RocksDBWrapper(false, transactionsDatabaseDirectory)) {
            this.progress.start("Building input transactions database");
            LineCleaner cleaner = (line) -> Utils.keepColumns(line, INPUTS_IMPORTANT);
            this.saveTransactions(inputsDirectory, database, null, cleaner, true);
            this.progress.stop();

            this.progress.start("Building output transactions database");
            LineFilter filter = (line) -> Utils.equalsAtColumn(line, "0", IS_FROM_COINBASE);
            cleaner = (line) -> Utils.keepColumns(line, OUTPUTS_IMPORTANT);
            this.saveTransactions(outputsDirectory, database, filter, cleaner, false);
            this.progress.done();
        }
    }

    private void saveTransactions(Path sourcesDirectory, RocksDBWrapper database, LineFilter filter, LineCleaner cleaner, boolean isInput) throws IOException, RocksDBException {
        final File[] sources = sourcesDirectory.toFile().listFiles((d, s) -> s.endsWith(".tsv"));

        if (sources == null) {
            throw new NoSuchFileException("Download inputs and outputs first");
        }

        for (String[] line : Utils.readTSVs(sources, filter, cleaner)) {
            long addressId = Utils.hashCode(line[isInput ? 0 : 1]);
            long transactionId = Utils.hashCode(line[isInput ? 1 : 0]);

            // System.out.println(addressId + " - " + transactionId);

            database.add(isInput ? INPUT : OUTPUT, Utils.longToBytes(transactionId), Utils.longToBytes(addressId));
            this.progress.lightUpdate();
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, RocksDBException {
        new TransactionsDatabase().compute();
    }
}
