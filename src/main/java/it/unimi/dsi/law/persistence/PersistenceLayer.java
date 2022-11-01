package it.unimi.dsi.law.persistence;

import it.unimi.dsi.law.Parameters;
import org.rocksdb.*;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class PersistenceLayer implements Closeable {
    private final ColumnFamilyOptions columnOptions;
    private final DBOptions options;

    public final List<ColumnFamilyHandle> columnFamilyHandleList;
    public final RocksDB db;

    private final AddressConversion ac;
    private final IncompleteMappings im;
    private final TransactionOutpointFilter tof;

    public PersistenceLayer(String location) throws RocksDBException {
        RocksDB.loadLibrary();

        columnOptions = new ColumnFamilyOptions()
                .optimizeUniversalStyleCompaction()
                .setMergeOperator(new StringAppendOperator(""));

        final List<ColumnFamilyDescriptor> columnFamilyDescriptors = Arrays.asList(
                new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, columnOptions),
                new ColumnFamilyDescriptor("address-conversion".getBytes(), columnOptions),
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

        db = RocksDB.open(options, location, columnFamilyDescriptors, columnFamilyHandleList);

        ac = new AddressConversion(db, columnFamilyHandleList.get(1));
        im = new IncompleteMappings(db, columnFamilyHandleList.get(2));
        tof = new TransactionOutpointFilter(db, columnFamilyHandleList.get(3));
    }

    public AddressConversion getAddressConversion() {
        return ac;
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

    public List<byte[]> keys() {
        List<byte[]> keys = new ArrayList<>();

        for (ColumnFamilyHandle column : columnFamilyHandleList) {
            RocksIterator rit = this.db.newIterator(column);
            rit.seekToFirst();

            while (rit.isValid()) {
                keys.add(rit.key());
                rit.next();
            }
        }

        return keys;
    }

    public boolean containsAnywhere(byte[] key) throws RocksDBException {
        for (ColumnFamilyHandle column : columnFamilyHandleList) {
            if (this.db.get(column, key) != null)
                return true;
        }
        return false;
    }

    public void close() {
        columnFamilyHandleList.forEach(ColumnFamilyHandle::close);

        options.close();
        db.close();
        columnOptions.close();
    }
}
