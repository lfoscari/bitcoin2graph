package it.unimi.dsi.law.persistence;

import com.google.common.collect.Lists;
import com.google.common.primitives.Bytes;
import it.unimi.dsi.law.utils.ByteConversion;
import org.apache.commons.collections4.list.FixedSizeList;
import org.bitcoinj.core.Sha256Hash;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDBException;
import org.rocksdb.WriteBatch;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

public class TransactionAddresses {
	static int LENGTH = 20;

	public static List<byte[]> get (PersistenceLayer mappings, Sha256Hash txId, Date date) throws RocksDBException {
		ColumnFamilyHandle column = mappings.getColumnFamilyHandleList().get(3);

		byte[] dateBytes = Bytes.ensureCapacity(ByteConversion.long2bytes(date.getTime()), Long.BYTES, 0);
		byte[] key = ByteConversion.concat(dateBytes, txId.getBytes());

		byte[] value = mappings.db.get(column, key);

		if (value == null) {
			return List.of();
		}

		List<byte[]> addresses = new ArrayList<>(value.length / LENGTH);
		for (int i = 0; i < value.length; i += LENGTH) {
			addresses.add(Arrays.copyOfRange(value, i, i + LENGTH));
		}
		return addresses;
	}

	public static void put (WriteBatch wb, ColumnFamilyHandle column, Sha256Hash txId, List<byte[]> addresses, Date date) throws RocksDBException {
		byte[] dateBytes = Bytes.ensureCapacity(ByteConversion.long2bytes(date.getTime()), Long.BYTES, 0);
		byte[] key = ByteConversion.concat(dateBytes, txId.getBytes());

		byte[] value = ByteConversion.concat(addresses);

		wb.merge(column, key, value);
	}
}