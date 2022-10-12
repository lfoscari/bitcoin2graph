package it.unimi.dsi.law;

import org.bitcoinj.core.Address;
import org.bitcoinj.core.Sha256Hash;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

import java.io.Closeable;

// TODO: implement strong persistence in case of crash, check out column families.
// https://github.com/facebook/rocksdb/wiki/RocksJava-Basics
// https://github.com/facebook/rocksdb/wiki/Tuning-RocksDB-from-Java

/**
 * Map addresses by assigning an increasing index.
 * Keep the association in a RocksDB instance
 */
public class AddressConversion implements Closeable {
    private final RocksDB db;
    private long count = 0;

    public AddressConversion() {
        RocksDB.loadLibrary();

        try (final Options options = new Options().setCreateIfMissing(true)) {
            this.db = RocksDB.open(options, "/tmp/rocksdb");
        } catch (RocksDBException e) {
            throw new RuntimeException(e);
        }
    }

    public long map(Address a) {
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

    public void close() {
        this.db.close();
    }

    private byte[] long2bytes(long l) {
        byte[] bb = new byte[8];
        int i, shift;
        for (i = 0, shift = 56; i < 8; i++, shift -= 8)
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
