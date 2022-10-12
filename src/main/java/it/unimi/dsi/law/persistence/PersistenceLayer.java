package it.unimi.dsi.law.persistence;

import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

import java.io.Closeable;

// TODO: implement strong persistence in case of crash, check out column families.
// https://github.com/facebook/rocksdb/wiki/RocksJava-Basics
// https://github.com/facebook/rocksdb/wiki/Tuning-RocksDB-from-Java

public class PersistenceLayer implements Closeable {
    private final RocksDB db;
    private static PersistenceLayer pl = null;

    private static AddressConversion ac = null;
    private static IncompleteMappings im = null;
    private static TransactionOutpointFilter tof = null;

    private PersistenceLayer() throws RocksDBException {
        RocksDB.loadLibrary();
        final Options options = new Options().setCreateIfMissing(true);
        this.db = RocksDB.open(options, "/tmp/rocksdb");
    }

    public static PersistenceLayer getInstance() {
        if (pl == null) {
            try {
                pl = new PersistenceLayer();
            } catch (RocksDBException e) {
                throw new RuntimeException(e);
            }
        }

        ac = new AddressConversion(pl.db);
        im = new IncompleteMappings(pl.db);
        tof = new TransactionOutpointFilter(pl.db);

        return pl;
    }

    public static AddressConversion getAddressConversion() {
        return ac;
    }

    public static IncompleteMappings getIncompleteMappings() {
        return im;
    }

    public static TransactionOutpointFilter getTransactionOutpointFilter() {
        return tof;
    }

    public void close() {
        this.db.close();
    }
}
