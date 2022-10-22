package it.unimi.dsi.law.persistence;

import org.rocksdb.*;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class PersistenceLayer implements Closeable {
    final long WRITE_BUFFER_SIZE  = 2L << 5; // 32 GB
    final long MAX_TOTAL_WAL_SIZE = 2L << 10; // 1024 GB

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

        columnOptions = new ColumnFamilyOptions()
                .optimizeUniversalStyleCompaction();

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
                .setDbWriteBufferSize(WRITE_BUFFER_SIZE)
                .setMaxTotalWalSize(MAX_TOTAL_WAL_SIZE);

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
        columnFamilyHandleList.forEach(ColumnFamilyHandle::close);

        options.close();
        db.close();
        columnOptions.close();
    }
}
