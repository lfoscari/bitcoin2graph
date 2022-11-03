package it.unimi.dsi.law.persistence;

import it.unimi.dsi.law.Parameters;
import org.rocksdb.*;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static it.unimi.dsi.law.Parameters.MAX_BYTES_FOR_LEVEL_BASE;

public class PersistenceLayer implements Closeable {
    private final String location;
    private final ColumnFamilyOptions columnOptions;
    private final DBOptions options;

    public final List<ColumnFamilyHandle> columnFamilyHandleList;
    public final List<ColumnFamilyDescriptor> columnFamilyDescriptors;
    public RocksDB db;

    private final IncompleteMappings im;
    private final TransactionOutpointFilter tof;

    public PersistenceLayer(String location) throws RocksDBException {
        this(location, false);
    }
    public PersistenceLayer(String location, boolean readonly) throws RocksDBException {
        RocksDB.loadLibrary();

        this.location = location;

        columnOptions = new ColumnFamilyOptions()
                .optimizeUniversalStyleCompaction()
                .setMaxBytesForLevelBase(MAX_BYTES_FOR_LEVEL_BASE)
                .setMergeOperator(new StringAppendOperator(""));

        columnFamilyDescriptors = Arrays.asList(
                new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, columnOptions),
                new ColumnFamilyDescriptor("incomplete-mappings".getBytes(), columnOptions),
                new ColumnFamilyDescriptor("transaction-outpoint-filter".getBytes(), columnOptions)
        );

        columnFamilyHandleList = new ArrayList<>();

        options = new DBOptions()
                .setCreateIfMissing(true)
                .setCreateMissingColumnFamilies(true)
                .setDbWriteBufferSize(Parameters.WRITE_BUFFER_SIZE)
                .setMaxTotalWalSize(Parameters.MAX_TOTAL_WAL_SIZE)
                .setMaxBackgroundJobs(Parameters.MAX_BACKGROUND_JOBS);

        if (readonly) {
            db = RocksDB.openReadOnly(options, location, columnFamilyDescriptors, columnFamilyHandleList);
        } else {
            db = RocksDB.open(options, location, columnFamilyDescriptors, columnFamilyHandleList);
        }

        im = new IncompleteMappings(db, columnFamilyHandleList.get(1));
        tof = new TransactionOutpointFilter(db, columnFamilyHandleList.get(2));
    }

    public IncompleteMappings getIncompleteMappings() {
        return im;
    }

    public TransactionOutpointFilter getTransactionOutpointFilter() {
        return tof;
    }

    public void mergeWith(PersistenceLayer other) throws RocksDBException {
        try (WriteBatch wb = new WriteBatch()) {
            for (ColumnFamilyHandle column : other.columnFamilyHandleList) {
                RocksIterator rit = other.db.newIterator(column);
                rit.seekToFirst();

                while (rit.isValid()) {
                    wb.merge(column, rit.key(), rit.value());
                    rit.next();
                }
            }

            this.db.write(new WriteOptions(), wb);
        }
    }

    public void delete() {
        this.close();
        deleteDirectory(new File(this.location));
    }

    public void close() {
        columnFamilyHandleList.forEach(ColumnFamilyHandle::close);

        options.close();
        db.close();
        columnOptions.close();
    }

    private boolean deleteDirectory(File directoryToBeDeleted) {
        File[] allContents = directoryToBeDeleted.listFiles();
        if (allContents != null)
            for (File file : allContents)
                deleteDirectory(file);
        return directoryToBeDeleted.delete();
    }
}
