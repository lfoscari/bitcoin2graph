package it.unimi.dsi.law.persistence;

import org.bitcoinj.core.Address;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

public class AddressConversion {
    private final RocksDB db;
    private final ColumnFamilyHandle column;

    public long count = 1;

    public AddressConversion(RocksDB db, ColumnFamilyHandle column) {
        this.db = db;
        this.column = column;
    }

    public long mapAddress(Address a) throws RocksDBException {
        byte[] key = a.getHash();
        byte[] value = db.get(column, key);

        if (value == null) {
            db.put(column, key, ByteConversion.long2bytes(count));
            return count++;
        }

        return ByteConversion.bytes2long(value);
    }
}