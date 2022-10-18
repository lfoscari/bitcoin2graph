package it.unimi.dsi.law.persistence;

import org.rocksdb.*;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

// TODO: implement strong persistence in case of crash, check out column families.
// https://github.com/facebook/rocksdb/wiki/RocksJava-Basics
// https://github.com/facebook/rocksdb/wiki/Tuning-RocksDB-from-Java

public class PersistenceLayer implements Closeable {
    private static PersistenceLayer pl = null;

    private final ColumnFamilyOptions columnOptions;
    private final List<ColumnFamilyHandle> columnFamilyHandleList;
    private final DBOptions options;
    private final RocksDB db;

    private final AddressConversion ac;
    private final IncompleteMappings im;
    private final TransactionOutpointFilter tof;

    private PersistenceLayer(String location) throws RocksDBException {
        RocksDB.loadLibrary();

        columnOptions = new ColumnFamilyOptions().optimizeUniversalStyleCompaction();

        final List<ColumnFamilyDescriptor> columnFamilyDescriptors = Arrays.asList(
                new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, columnOptions),
                new ColumnFamilyDescriptor("address-conversion".getBytes(), columnOptions),
                new ColumnFamilyDescriptor("incomplete-mappings".getBytes(), columnOptions),
                new ColumnFamilyDescriptor("transaction-outpoint-filter".getBytes(), columnOptions)
        );

        columnFamilyHandleList = new ArrayList<>();

        options = new DBOptions().setCreateIfMissing(true).setCreateMissingColumnFamilies(true);
        db = RocksDB.open(options, location, columnFamilyDescriptors, columnFamilyHandleList);

        ac = new AddressConversion(db, columnFamilyHandleList.get(1));
        im = new IncompleteMappings(db, columnFamilyHandleList.get(2));
        tof = new TransactionOutpointFilter(db, columnFamilyHandleList.get(3));
    }

    public static PersistenceLayer getInstance(String location) throws RocksDBException {
        if (pl == null)
            pl = new PersistenceLayer(location);

        return pl;
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

    public void close() {
        for (final ColumnFamilyHandle columnFamilyHandle : columnFamilyHandleList)
            columnFamilyHandle.close();
        columnFamilyHandleList.forEach(ColumnFamilyHandle::close);

        options.close();
        db.close();
        columnOptions.close();
    }
}
