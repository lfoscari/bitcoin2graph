package it.unimi.dsi.law.persistence;

import it.unimi.dsi.law.utils.ByteConversion;
import org.bitcoinj.core.NetworkParameters;
import org.bitcoinj.core.Sha256Hash;
import org.bitcoinj.core.TransactionOutPoint;
import org.bitcoinj.params.MainNetParams;
import org.rocksdb.*;

import java.util.List;

public class TransactionOutpointFilter {
    private final RocksDB db;
    private final ColumnFamilyHandle column;

    private WriteBatch wb;
    private final long maxSize = 10000;

    public TransactionOutpointFilter(RocksDB db, ColumnFamilyHandle column) {
        this.db = db;
        this.column = column;
        this.wb = new WriteBatch();
    }

    public void put(Sha256Hash hash, Long index) throws RocksDBException {
        byte[] key = hash.getBytes();
        byte[] value = ByteConversion.long2bytes(index);

        this.wb.merge(column, key, value);

        if (this.wb.getDataSize() >= maxSize) {
            this.db.write(new WriteOptions(), this.wb);
            this.wb = new WriteBatch();
        }
    }

    public List<Long> get(Sha256Hash hash) throws RocksDBException {
        byte[] key = hash.getBytes();
        byte[] value = db.get(column, key);

        if (value == null)
            return List.of();

        return ByteConversion.bytes2longList(value);
    }
}