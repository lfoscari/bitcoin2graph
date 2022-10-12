package it.unimi.dsi.law.persistence;

import org.apache.commons.math3.analysis.function.Add;
import org.bitcoinj.core.TransactionOutPoint;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

import java.util.ArrayList;
import java.util.List;

public class IncompleteMappings {
    private final RocksDB db;
    private final ColumnFamilyHandle column;

    public IncompleteMappings(RocksDB db, ColumnFamilyHandle column) {
        this.db = db;
        this.column = column;
    }

    public void put(TransactionOutPoint top, List<Long> addresses) throws RocksDBException {
        byte[] key = top.getHash().getBytes();
        byte[] value = new byte[8 * addresses.size()];

        for (int i = 0; i < addresses.size(); i++) {
            byte[] ad = AddressConversion.long2bytes(addresses.get(i));
            System.arraycopy(ad, 0, value, i, 8);
        }

        db.put(column, key, value);
    }

    public List<Long> get(TransactionOutPoint top) throws RocksDBException {
        byte[] key = top.getHash().getBytes();
        byte[] value = db.get(column, key);

        List<Long> result = new ArrayList<>();
        for (int i = 0; i < ; i++) {
            result += AddressConversion.bytes2long(value[]);
        }

        return result;
    }
}