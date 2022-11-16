package it.unimi.dsi.law.persistence;

import it.unimi.dsi.fastutil.Pair;
import it.unimi.dsi.law.utils.ByteConversion;
import org.bitcoinj.core.Sha256Hash;
import org.bitcoinj.core.TransactionOutPoint;
import org.rocksdb.*;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class IncompleteMappings {
	static int PAIR_LENGTH = Sha256Hash.LENGTH + Long.BYTES;

	public static List<Pair<Sha256Hash, Long>> get (PersistenceLayer mappings, Sha256Hash txId) throws RocksDBException {
		ColumnFamilyHandle column = mappings.getColumnFamilyHandleList().get(1);

		byte[] key = txId.getBytes();
		byte[] value = mappings.db.get(column, key);

		return parse(value);
	}

	public static List<Pair<Sha256Hash, Long>> parse (byte[] value) {
		if (value == null)
			return null;

		List<Pair<Sha256Hash, Long>> result = new ArrayList<>();

		for (int i = 0; i < value.length; i += PAIR_LENGTH) {
			Sha256Hash topHash = Sha256Hash.wrap(Arrays.copyOfRange(value, i, i + Sha256Hash.LENGTH));
			Long topIndex = ByteConversion.bytes2long(Arrays.copyOfRange(value, i + Sha256Hash.LENGTH, i + PAIR_LENGTH));

			result.add(Pair.of(topHash, topIndex));
		}

		return result;
	}

	public static void put (WriteBatch wb, ColumnFamilyHandle column, Sha256Hash txId, TransactionOutPoint top) throws RocksDBException {
		byte[] key = txId.getBytes();
		byte[] value = ByteConversion.concat(top.getHash().getBytes(), ByteConversion.long2bytes(top.getIndex()));

		wb.merge(column, key, value);
	}
}