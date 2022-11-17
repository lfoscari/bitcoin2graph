package it.unimi.dsi.law.persistence;

import it.unimi.dsi.law.utils.ByteConversion;
import org.bitcoinj.core.LegacyAddress;
import org.bitcoinj.core.Sha256Hash;
import org.rocksdb.*;

import java.util.List;

public class Arcs {
	public static List<byte[]> get (PersistenceLayer mappings, byte[] sender) throws RocksDBException {
		ColumnFamilyHandle column = mappings.getColumnFamilyHandleList().get(3);

		byte[] receivers = mappings.db.get(column, sender);

		if (receivers == null) {
			return List.of();
		}

		return ByteConversion.partition(receivers, LegacyAddress.LENGTH);
	}

	public static void put (WriteBatch wb, ColumnFamilyHandle column, byte[] sender, byte[] receiver) throws RocksDBException {
		wb.merge(column, sender, receiver);
	}

	public static void put (WriteBatch wb, ColumnFamilyHandle column, byte[] sender, List<byte[]> receivers) throws RocksDBException {
		wb.merge(column, sender, ByteConversion.concat(receivers));
	}

}
