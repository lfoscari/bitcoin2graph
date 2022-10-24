package it.unimi.dsi.law.persistence;

import it.unimi.dsi.law.utils.ByteConversion;
import org.bitcoinj.core.TransactionOutPoint;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.TransactionDB;

import java.util.List;

public class IncompleteMappings {
    private final RocksDB db;
    private final ColumnFamilyHandle column;

    public IncompleteMappings(RocksDB db, ColumnFamilyHandle column) {
        this.db = db;
        this.column = column;
    }

    public void put(TransactionOutPoint top, List<Long> addresses) throws RocksDBException {
        byte[] key = ByteConversion.int2bytes(top.hashCode());
        byte[] value = ByteConversion.longList2bytes(addresses);

        db.merge(column, key, value);
    }

    public List<Long> get(TransactionOutPoint top) throws RocksDBException {
        byte[] key = ByteConversion.int2bytes(top.hashCode());
        byte[] value = db.get(column, key);

        if (value == null)
            return List.of();

        return ByteConversion.bytes2longList(value);
    }
}