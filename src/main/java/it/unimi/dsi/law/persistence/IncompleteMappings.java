package it.unimi.dsi.law.persistence;

import it.unimi.dsi.law.utils.ByteConversion;
import org.bitcoinj.core.TransactionOutPoint;
import org.rocksdb.*;

import java.util.List;

public class IncompleteMappings {
    private final RocksDB db;
    private final ColumnFamilyHandle column;

    private WriteBatch wb;
    private final long maxSize = 10000;

    public IncompleteMappings(RocksDB db, ColumnFamilyHandle column) {
        this.db = db;
        this.column = column;
        this.wb = new WriteBatch();
    }

    public void put(TransactionOutPoint top, List<Long> addresses) throws RocksDBException {
        byte[] key = ByteConversion.int2bytes(top.hashCode());
        byte[] value = ByteConversion.longList2bytes(addresses);

        this.wb.merge(column, key, value);

        if (this.wb.getDataSize() >= maxSize) {
            this.db.write(new WriteOptions(), this.wb);
            this.wb = new WriteBatch();
        }
    }

    public List<Long> get(TransactionOutPoint top) throws RocksDBException {
        byte[] key = ByteConversion.int2bytes(top.hashCode());
        byte[] value = this.db.get(column, key);

        if (value == null)
            return List.of();

        return ByteConversion.bytes2longList(value);
    }
}