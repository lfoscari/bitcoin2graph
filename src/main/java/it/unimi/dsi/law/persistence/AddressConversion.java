package it.unimi.dsi.law.persistence;

import org.bitcoinj.core.Address;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

public class AddressConversion {
    private final RocksDB db;
    private long count = 0;

    public AddressConversion(RocksDB db) {
        this.db = db;
    }

    public long mapAddress(Address a) {
        byte[] key = a.getHash();

        try {
            byte[] value = db.get(key);

            if (value == null) {
                db.put(key, long2bytes(count));
                return count++;
            }

            return bytes2long(value);
        } catch (RocksDBException e) {
            throw new RuntimeException(e);
        }
    }

    private byte[] long2bytes(long l) {
        byte[] bb = new byte[8];
        for (int i = 0, shift = 56; i < 8; i++, shift -= 8)
            bb[i] = (byte) (0xFF & (l >> shift));
        return bb;
    }

    private long bytes2long(byte[] bb) {
        long n = 0L;
        for (byte b : bb) {
            n <<= 8;
            n |= (b & 0xff);
        }
        return n;
    }
}