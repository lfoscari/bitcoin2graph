package it.unimi.dsi.law.persistence;

import org.bitcoinj.core.Address;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

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

    public static byte[] trim(byte[] bb) {
        int z = 0;
        while(z < 8 && bb[z++] == 0);
        return Arrays.copyOfRange(bb, z - 1, 8);
    }

    public static long bytes2long(byte[] bb) {
        long n = 0L;
        for (byte b : bb) {
            n <<= 8;
            n |= (b & 0xff);
        }
        return n;
    }

    public static byte[] longList2bytes(List<Long> ll) {
        byte[] bb = new byte[8 * ll.size()];

        for (int i = 0; i < ll.size(); i++) {
            Long v = ll.get(i);
            byte[] tt = AddressConversion.long2bytes(v != null ? v : -1L);
            System.arraycopy(tt, 0, bb, i * 8, 8);
        }

        return bb;
    }

    public static List<Long> bytes2longList(byte[] bb) {
        List<Long> l = new ArrayList<>();

        for (int i = 0; i < bb.length; i += 8) {
            byte[] el = new byte[8];
            System.arraycopy(bb, i, el, 0, 8);
            long t = AddressConversion.bytes2long(el);
            l.add(t == -1L ? null : t);
        }

        return l;
    }
}