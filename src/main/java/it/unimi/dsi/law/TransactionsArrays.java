package it.unimi.dsi.law;

import it.unimi.dsi.fastutil.io.BinIO;
import it.unimi.dsi.fastutil.longs.LongArrays;
import it.unimi.dsi.fastutil.longs.LongSet;
import it.unimi.dsi.fastutil.objects.Object2LongFunction;
import it.unimi.dsi.fastutil.objects.ObjectArrays;
import it.unimi.dsi.logging.ProgressLogger;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.WriteBatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.NoSuchElementException;

import static it.unimi.dsi.law.Parameters.*;
import static it.unimi.dsi.law.Utils.*;

public class TransactionsArrays {
    private final Object2LongFunction<CharSequence> addressMap;

    private RocksDB db;
    private Options options;

    private final ProgressLogger progress;

    public TransactionsArrays() throws IOException, ClassNotFoundException {
        this(null);
    }

    public TransactionsArrays(ProgressLogger progress) throws IOException, ClassNotFoundException {
        if (progress == null) {
            Logger logger = LoggerFactory.getLogger(Blockchain2Webgraph.class);
            progress = new ProgressLogger(logger, logInterval, logTimeUnit, "sources");
            progress.displayLocalSpeed = true;
        }

        this.progress = progress;
        this.addressMap = (Object2LongFunction<CharSequence>) BinIO.loadObject(addressesMapFile.toFile());
    }

    void compute() throws IOException, RocksDBException {
        this.progress.start("Building input transactions arrays");

        {
            LineFilter filter = (line) -> true;
            LineCleaner cleaner = (line) -> Utils.keepImportant(line, INPUTS_IMPORTANT);
            this.saveTransactions(inputsDirectory, inputTransactionDatabaseDirectory, filter, cleaner);
        }

        {
            LineFilter filter = (line) -> line[BitcoinColumn.IS_FROM_COINBASE].equals("0");
            LineCleaner cleaner = (line) -> Utils.keepImportant(line, OUTPUTS_IMPORTANT);
            this.saveTransactions(outputsDirectory, outputTransactionDatabaseDirectory, filter, cleaner);
        }

        this.progress.done();
    }

    private void saveTransactions(Path sourcesDirectory, Path databaseDirectory, LineFilter filter, LineCleaner cleaner) throws IOException, RocksDBException {
        File[] sources = sourcesDirectory.toFile().listFiles();

        if (sources == null) {
            throw new NoSuchFileException("Download inputs and outputs first");
        }

        this.db = this.startDatabase(false, databaseDirectory);
        WriteBatch wb = new WriteBatch();

        TSVDirectoryLineReader sourcesReader = new TSVDirectoryLineReader(sources, filter, cleaner, true, this.progress);
        while (true) {
            try {
                String[] line = sourcesReader.next();
                String transaction = line[0], address = line[1];

                long addressId = this.addressMap.getLong(address);


            } catch (NoSuchElementException e) {
                break;
            }
        }

        this.closeDatabase();
    }

    private RocksDB startDatabase(boolean readonly, Path location) throws RocksDBException {
        RocksDB.loadLibrary();

        this.options = new Options()
                .setCreateIfMissing(true)
                .setCreateMissingColumnFamilies(true)
                .setDbWriteBufferSize(WRITE_BUFFER_SIZE)
                .setMaxTotalWalSize(MAX_TOTAL_WAL_SIZE)
                .setMaxBackgroundJobs(MAX_BACKGROUND_JOBS);

        if (readonly) {
            return RocksDB.openReadOnly(this.options, location.toString());
        }

        return RocksDB.open(this.options, location.toString());
    }

    public void closeDatabase() {
        this.options.close();
        this.db.close();
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, RocksDBException {
        new TransactionsArrays().compute();
    }
}
