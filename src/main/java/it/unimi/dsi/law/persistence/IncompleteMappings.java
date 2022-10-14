package it.unimi.dsi.law.persistence;

import org.bitcoinj.core.TransactionOutPoint;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

import java.nio.ByteBuffer;
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

        byte[] key = AddressConversion.int2bytes(top.hashCode());
        byte[] value = new byte[] {};

        if (db.get(column, key) != null) {
            value = db.get(column, key);
            byte[] newValue = AddressConversion.longList2bytes(ll);
            byte[] result = new byte[value.length + newValue.length];

            System.arraycopy(value, 0, result, 0, value.length);
            System.arraycopy(newValue, 0, result, value.length, newValue.length);

            value = result;
        } else {
            value = AddressConversion.longList2bytes(ll);
        }

        db.put(column, key, value);
    }

    public List<Long> get(TransactionOutPoint top) throws RocksDBException {
        byte[] key = AddressConversion.int2bytes(top.hashCode());
        byte[] value = db.get(column, key);

        if (value == null) return List.of();

        List<Long> ll = AddressConversion.bytes2longList(value);
        return ll.stream().map(a -> a == -1L ? null : a).collect(Collectors.toList());
    }
}