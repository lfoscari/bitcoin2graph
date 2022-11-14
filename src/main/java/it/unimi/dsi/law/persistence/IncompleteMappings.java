package it.unimi.dsi.law.persistence;

import com.google.common.primitives.Bytes;
import it.unimi.dsi.law.utils.ByteConversion;
import org.bitcoinj.core.TransactionOutPoint;
import org.rocksdb.*;

import java.util.Date;
import java.util.List;

public class IncompleteMappings {
	public static List<Long> get (PersistenceLayer mappings, TransactionOutPoint top, Date date) throws RocksDBException {
		ColumnFamilyHandle column = mappings.getColumnFamilyHandleList().get(1);

		byte[] dateBytes = Bytes.ensureCapacity(ByteConversion.long2bytes(date.getTime()), Long.BYTES, 0);
		byte[] key = ByteConversion.concat(dateBytes, ByteConversion.int2bytes(top.hashCode()));

		byte[] value = mappings.db.get(column, key);

		if (value == null) {
			return List.of();
		}

		return ByteConversion.bytes2longList(value);
	}

	public static void put (WriteBatch wb, ColumnFamilyHandle column, TransactionOutPoint top, List<Long> addresses, Date date) throws RocksDBException {
		byte[] dateBytes = Bytes.ensureCapacity(ByteConversion.long2bytes(date.getTime()), Long.BYTES, 0);
		byte[] key = ByteConversion.concat(dateBytes, ByteConversion.int2bytes(top.hashCode()));

		byte[] value = ByteConversion.longList2bytes(addresses);

		wb.merge(column, key, value);
	}
}