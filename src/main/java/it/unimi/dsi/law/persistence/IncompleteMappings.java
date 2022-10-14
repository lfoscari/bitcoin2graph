package it.unimi.dsi.law.persistence;

import org.bitcoinj.core.TransactionOutPoint;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class IncompleteMappings {
    private final RocksDB db;
    private final ColumnFamilyHandle column;

    public IncompleteMappings(RocksDB db, ColumnFamilyHandle column) {
        this.db = db;
        this.column = column;
    }

    public void put(TransactionOutPoint top, List<Long> addresses) throws RocksDBException {
        List<Long> ll = addresses.stream().map(a -> a != null ? a : -1L).collect(Collectors.toList());

        byte[] key = top.getHash().getBytes();
        // byte[] value = AddressConversion.longList2bytes(ll);

        byte[] oldValue = new byte[] {};
        // !!! la get copia in value il valore conservato nel db !!!
        if (db.get(column, key, oldValue) != RocksDB.NOT_FOUND) {
            byte[] newValue = AddressConversion.longList2bytes(ll);

            byte[] res = Arrays.copyOf(oldValue, oldValue.length + newValue.length);
            System.arraycopy(newValue, 0, res, oldValue.length, newValue.length);
            oldValue = res;
        } else {
            oldValue = AddressConversion.longList2bytes(ll);
        }

        db.put(column, key, oldValue);
    }

    public List<Long> get(TransactionOutPoint top) throws RocksDBException {
        byte[] key = top.getHash().getBytes();
        byte[] value = db.get(column, key);

        List<Long> ll = AddressConversion.bytes2longList(value);
        return ll.stream().map(a -> a == -1L ? null : a).collect(Collectors.toList());
    }
}