package it.unimi.dsi.law.persistence;

import it.unimi.dsi.law.utils.ByteConversion;
import org.bitcoinj.core.NetworkParameters;
import org.bitcoinj.core.Sha256Hash;
import org.bitcoinj.core.TransactionOutPoint;
import org.bitcoinj.params.MainNetParams;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.Holder;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

import java.util.List;

public class TransactionOutpointFilter {
    private final RocksDB db;
    private final ColumnFamilyHandle column;

    public TransactionOutpointFilter(RocksDB db, ColumnFamilyHandle column) {
        this.db = db;
        this.column = column;
    }

    public void put(Sha256Hash hash, Long index) throws RocksDBException {
        byte[] key = hash.getBytes();
        byte[] value = ByteConversion.long2bytes(index);

        db.merge(column, key, value);
    }

    public List<Long> get(Sha256Hash hash) throws RocksDBException {
        byte[] key = hash.getBytes();
        byte[] value = db.get(column, key);

        if (value == null)
            return List.of();

        return ByteConversion.bytes2longList(value);
    }

    public Holder<byte[]> keyMayExist(Sha256Hash hash) {
        Holder<byte[]> holder = new Holder<>();
        byte[] key = hash.getBytes();

        if (db.keyMayExist(column, key, holder))
            return holder;

        return null;
    }
}