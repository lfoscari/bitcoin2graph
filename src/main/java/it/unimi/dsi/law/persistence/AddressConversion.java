package it.unimi.dsi.law.persistence;

import it.unimi.dsi.law.utils.ByteConversion;
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

    public long map(byte[] key) throws RocksDBException {
        byte[] value = db.get(column, key);

        if (value == null) {
            db.put(column, key, ByteConversion.long2bytes(count));
            return count++;
        }

        return ByteConversion.bytes2long(value);
    }
}