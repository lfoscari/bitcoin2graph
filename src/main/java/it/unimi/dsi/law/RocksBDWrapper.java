package it.unimi.dsi.law;

import org.bitcoinj.core.Address;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

// TODO: implement strong persistence in case of crash, check out column families.
// https://github.com/facebook/rocksdb/wiki/RocksJava-Basics
// https://github.com/facebook/rocksdb/wiki/Tuning-RocksDB-from-Java

public class RocksBDWrapper {
    private final RocksDB db;

    public RocksBDWrapper() {
        RocksDB.loadLibrary();

        try (final Options options = new Options().setCreateIfMissing(true)) {
            this.db = RocksDB.open(options, "/tmp/rocksdb");
        } catch (RocksDBException e) {
            throw new RuntimeException(e);
        }
    }

    public void put(Address key, Long value) {
        try {
            db.put(key.getHash(), long2bytes(value));
        } catch (RocksDBException e) {
            throw new RuntimeException(e);
        }
    }

    public Long get(Address key) {
        try {
            byte[] value = db.get(key.getHash());
            return bytes2long(value);
        } catch (RocksDBException e) {
            throw new RuntimeException(e);
        }
    }

    public boolean containsKey(Address key) {
        // TODO: optimize by saving this result
        try {
            return db.get(key.getHash()) != null;
        } catch (RocksDBException e) {
            throw new RuntimeException(e);
        }
    }

    public void close() {
        this.db.close();
    }

    private byte[] long2bytes(long l) {
        byte[] array = new byte[8];
        int i, shift;
        for (i = 0, shift = 56; i < 8; i++, shift -= 8)
            array[i] = (byte) (0xFF & (l >> shift));
        return array;
    }

    private long bytes2long(byte[] b) {
        long value = 0L;
        for (int i = 0; i < b.length; i++) {
            value <<= 8;
            value |= (b[i] & 0xff);
        }
        return value;
    }
}
