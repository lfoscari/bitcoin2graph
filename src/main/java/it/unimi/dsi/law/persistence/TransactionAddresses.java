package it.unimi.dsi.law.persistence;

import com.google.common.collect.Lists;
import com.google.common.primitives.Bytes;
import it.unimi.dsi.law.utils.ByteConversion;
import org.apache.commons.collections4.list.FixedSizeList;
import org.bitcoinj.core.LegacyAddress;
import org.bitcoinj.core.Sha256Hash;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDBException;
import org.rocksdb.WriteBatch;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

public class TransactionAddresses {
	public static List<byte[]> get (PersistenceLayer mappings, Sha256Hash txId) throws RocksDBException {
		ColumnFamilyHandle column = mappings.getColumnFamilyHandleList().get(2);

		byte[] key = txId.getBytes();
		byte[] addresses = mappings.db.get(column, key);

		if (addresses == null) {
			return null;
		}

		return ByteConversion.partition(addresses, LegacyAddress.LENGTH);
	}

	public static void put (WriteBatch wb, ColumnFamilyHandle column, Sha256Hash txId, List<byte[]> addresses) throws RocksDBException {
		byte[] key = txId.getBytes();
		byte[] value = ByteConversion.concat(addresses);

		wb.merge(column, key, value);
	}

	public static byte[] getAtOffset (PersistenceLayer mappings, Sha256Hash txId, int index) throws RocksDBException {
		ColumnFamilyHandle column = mappings.getColumnFamilyHandleList().get(2);

		byte[] key = txId.getBytes();
		byte[] addresses = mappings.db.get(column, key);

		if (addresses == null) {
			return null;
		}

		return Arrays.copyOfRange(addresses, index * LegacyAddress.LENGTH, (index + 1) * LegacyAddress.LENGTH);
	}
}