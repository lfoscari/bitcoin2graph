package it.unimi.dsi.law.persistence;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class ByteConversion {
    public static byte[] long2bytes(long l) {
        return ByteBuffer.allocate(8).putLong(l).array();
    }

    public static byte[] int2bytes(int n) {
        return ByteBuffer.allocate(4).putInt(n).array();
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
            byte[] tt = long2bytes(ll.get(i));
            System.arraycopy(tt, 0, bb, i * 8, 8);
        }

        return bb;
    }

    public static List<Long> bytes2longList(byte[] bb) {
        List<Long> l = new ArrayList<>();

        for (int i = 0; i < bb.length; i += 8) {
            byte[] el = new byte[8];
            System.arraycopy(bb, i, el, 0, 8);
            l.add(bytes2long(el));
        }

        return l;
    }

    public static byte[] concat(byte[] aa, byte[] bb) {
        byte[] result = new byte[aa.length + bb.length];

        System.arraycopy(aa, 0, result, 0, aa.length);
        System.arraycopy(bb, 0, result, aa.length, bb.length);

        return result;
    }
}
