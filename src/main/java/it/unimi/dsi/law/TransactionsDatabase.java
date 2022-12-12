package it.unimi.dsi.law;

import it.unimi.dsi.fastutil.ints.Int2LongFunction;
import it.unimi.dsi.fastutil.io.BinIO;
import it.unimi.dsi.fastutil.objects.Object2LongFunction;
import it.unimi.dsi.logging.ProgressLogger;
import org.rocksdb.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.NoSuchElementException;

import static it.unimi.dsi.law.Parameters.*;
import static it.unimi.dsi.law.Parameters.BitcoinColumn.*;
import static it.unimi.dsi.law.Utils.*;

public class TransactionsDatabase {
    private final Int2LongFunction addressMap;
    private final Int2LongFunction transactionMap;

    private final int WB_LIMIT = 10_000;

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
        this.addressMap = (Int2LongFunction) BinIO.loadObject(addressesMapFile.toFile());
        this.transactionMap = (Int2LongFunction) BinIO.loadObject(transactionsMapFile.toFile());
    }

    void compute() throws IOException, RocksDBException {
        {
            this.progress.start("Building input transactions database");
            LineFilter filter = (line) -> true;
            LineCleaner cleaner = (line) -> Utils.keepImportant(line, INPUTS_IMPORTANT);
            this.saveTransactions(inputsDirectory, inputTransactionDatabaseDirectory, filter, cleaner);
            this.progress.stop();
        }

        {
            this.progress.start("Building output transactions database");
            LineFilter filter = (line) -> line[IS_FROM_COINBASE].equals("0");
            LineCleaner cleaner = (line) -> Utils.keepImportant(line, OUTPUTS_IMPORTANT);
            this.saveTransactions(outputsDirectory, outputTransactionDatabaseDirectory, filter, cleaner);
            this.progress.stop();
        }

        this.progress.done();
    }

    private void saveTransactions(Path sourcesDirectory, Path databaseDirectory, LineFilter filter, LineCleaner cleaner) throws IOException, RocksDBException {
        File[] sources = sourcesDirectory.toFile().listFiles((d, s) -> s.endsWith(".tsv"));

        if (sources == null) {
            throw new NoSuchFileException("Download inputs and outputs first");
        }

        RocksDB database = this.startDatabase(false, databaseDirectory);
        WriteBatch wb = new WriteBatch();

        for (String[] line : Utils.readTSVs(sources, filter, cleaner, true, this.progress)) {
            long addressId = this.addressMap.get(line[1].hashCode());
            long transactionId = this.transactionMap.get(line[0].hashCode());

            wb.merge(Utils.longToBytes(transactionId), Utils.longToBytes(addressId));
            this.progress.lightUpdate();

            if (wb.getDataSize() >= this.WB_LIMIT) {
                database.write(new WriteOptions(), wb);
                wb = new WriteBatch();
            }
        }

        database.write(new WriteOptions(), wb);
        database.close();
    }

    private RocksDB startDatabase(boolean readonly, Path location) throws RocksDBException {
        RocksDB.loadLibrary();

        try (Options options = new Options()
                .setCreateIfMissing(true)
                .setCreateMissingColumnFamilies(true)
                .setMergeOperator(new StringAppendOperator())
                .setDbWriteBufferSize(WRITE_BUFFER_SIZE)
                .setMaxTotalWalSize(MAX_TOTAL_WAL_SIZE)
                .setMaxBackgroundJobs(MAX_BACKGROUND_JOBS)) {

            if (readonly) {
                return RocksDB.openReadOnly(options, location.toString());
            }

            return RocksDB.open(options, location.toString());
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, RocksDBException {
        new TransactionsDatabase().compute();
    }
}
