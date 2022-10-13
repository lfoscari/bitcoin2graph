package it.unimi.dsi.law.persistence;

import org.bitcoinj.core.Address;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

public class AddressConversion {
    private final RocksDB db;
    private final ColumnFamilyHandle column;

    private long count = 0;

    public AddressConversion(RocksDB db, ColumnFamilyHandle column) {
        this.db = db;
        this.column = column;
    }

    public long mapAddress(Address a) throws RocksDBException {
        byte[] key = a.getHash();
        byte[] value;

        value = db.get(column, key);

        if (value == null) {
            db.put(column, key, long2bytes(count));
            return count++;
        }

        return bytes2long(value);
    }

    public static byte[] long2bytes(long l) {
        byte[] bb = new byte[8];
        for (int i = 0, shift = 56; i < 8; i++, shift -= 8)
            bb[i] = (byte) (0xFF & (l >> shift));
        return bb;
    }

    public static long bytes2long(byte[] bb) {
        long n = 0L;
        for (byte b : bb) {
            n <<= 8;
            n |= (b & 0xff);
        }
        return n;
    }
}