package it.unimi.dsi.law.utils;

import com.google.common.primitives.Bytes;
import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import org.bitcoinj.core.LegacyAddress;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class ByteConversion {
	public static byte[] long2bytes (long l) {
		return Longs.toByteArray(l);
	}

	public static byte[] int2bytes (int n) {
		return Ints.toByteArray(n);
	}

	public static int bytes2int (byte[] bb) {
        if (bb.length > 4) {
            throw new ArithmeticException("byte array too big to be an integer");
        }

		bb = Bytes.ensureCapacity(bb, 4, 0);
		return Ints.fromBytes(bb[0], bb[1], bb[2], bb[3]);
	}

	public static long bytes2long (byte[] bb) {
        if (bb.length > 8) {
            throw new ArithmeticException("byte array too big to be a long");
        }

		bb = Bytes.ensureCapacity(bb, 8, 0);
		return Longs.fromBytes(bb[0], bb[1], bb[2], bb[3], bb[4], bb[5], bb[6], bb[7]);
	}

	public static byte[] longList2bytes (List<Long> ll) {
		byte[] bb = new byte[8 * ll.size()];

		for (int i = 0; i < ll.size(); i++) {
			byte[] tt = long2bytes(ll.get(i));
			System.arraycopy(tt, 0, bb, i * 8, 8);
		}

		return bb;
	}

	public static List<Long> bytes2longList (byte[] bb) {
		List<Long> l = new ArrayList<>();

		for (int i = 0; i < bb.length; i += 8) {
			byte[] el = new byte[8];
			System.arraycopy(bb, i, el, 0, 8);
			l.add(bytes2long(el));
		}

		return l;
	}

	public static byte[] concat (byte[] aa, byte[] bb) {
		byte[] result = new byte[aa.length + bb.length];

		System.arraycopy(aa, 0, result, 0, aa.length);
		System.arraycopy(bb, 0, result, aa.length, bb.length);

		return result;
	}

	public static byte[] concat (List<byte[]> bbb) {
		byte[] result = new byte[bbb.stream().mapToInt(bb -> bb.length).sum()];
		int i = 0;

		for (byte[] bb : bbb) {
			System.arraycopy(bb, 0, result, i, bb.length);
			i += bb.length;
		}

		return result;
	}

	public static List<byte[]> partition(byte[] bb, int length) {
		List<byte[]> addresses = new ArrayList<>(bb.length / length);
		for (int i = 0; i < bb.length; i += length) {
			addresses.add(Arrays.copyOfRange(bb, i, i + length));
		}
		return addresses;
	}
}
