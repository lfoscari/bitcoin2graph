package it.unimi.dsi.law.persistence;

import org.bitcoinj.core.TransactionOutPoint;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

import java.util.List;

public class IncompleteMappings {
    private final RocksDB db;
    private final ColumnFamilyHandle column;

    // This class has some issue
    // Maybe to some testing

    public IncompleteMappings(RocksDB db, ColumnFamilyHandle column) {
        this.db = db;
        this.column = column;
    }

    public void put(TransactionOutPoint top, List<Long> addresses) throws RocksDBException {
        byte[] key = top.getHash().getBytes();
        byte[] value = AddressConversion.longList2bytes(addresses);

        db.put(column, key, value);
    }

    public List<Long> get(TransactionOutPoint top) throws RocksDBException {
        byte[] key = top.getHash().getBytes();
        byte[] value = db.get(column, key);

        return AddressConversion.bytes2longList(value);
    }
}