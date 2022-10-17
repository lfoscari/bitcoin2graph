package it.unimi.dsi.law.persistence;

import com.google.common.primitives.Bytes;
import org.bitcoinj.core.TransactionOutPoint;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

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

        byte[] key = ByteConversion.int2bytes(top.hashCode());
        byte[] value = Bytes.concat(ByteConversion.longList2bytes(ll), ByteConversion.longList2bytes(get(top)));

        db.put(column, key, value);
    }

    public List<Long> get(TransactionOutPoint top) throws RocksDBException {
        byte[] key = ByteConversion.int2bytes(top.hashCode());
        byte[] value = db.get(column, key);

        if (value == null) return List.of();

        List<Long> ll = ByteConversion.bytes2longList(value);
        return ll.stream().map(a -> a == -1L ? null : a).collect(Collectors.toList());
    }
}