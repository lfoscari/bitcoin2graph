package it.unimi.dsi.law.persistence;

import com.google.common.primitives.Bytes;
import it.unimi.dsi.law.utils.ByteConversion;
import org.bitcoinj.core.Sha256Hash;
import org.rocksdb.*;

import java.util.Date;
import java.util.List;

public class TransactionOutpointFilter {
	public static List<Long> get (PersistenceLayer mappings, Sha256Hash hash, Date date) throws RocksDBException {
		ColumnFamilyHandle column = mappings.getColumnFamilyHandleList().get(2);

		byte[] dateBytes = Bytes.ensureCapacity(ByteConversion.long2bytes(date.getTime()), Long.BYTES, 0);
		byte[] key = ByteConversion.concat(dateBytes, hash.getBytes());

		byte[] value = mappings.db.get(column, key);

        if (value == null) {
            return List.of();
        }

		return ByteConversion.bytes2longList(value);
	}

	public static void put (WriteBatch wb, ColumnFamilyHandle column, Sha256Hash hash, Long index, Date date) throws RocksDBException {
		byte[] dateBytes = Bytes.ensureCapacity(ByteConversion.long2bytes(date.getTime()), Long.BYTES, 0);
		byte[] key = ByteConversion.concat(dateBytes, hash.getBytes());

		byte[] value = ByteConversion.long2bytes(index);

		wb.merge(column, key, value);
	}
}