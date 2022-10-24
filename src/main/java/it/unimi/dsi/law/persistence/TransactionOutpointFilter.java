package it.unimi.dsi.law.persistence;

import it.unimi.dsi.law.utils.ByteConversion;
import org.bitcoinj.core.NetworkParameters;
import org.bitcoinj.core.Sha256Hash;
import org.bitcoinj.core.TransactionOutPoint;
import org.bitcoinj.params.MainNetParams;
import org.bouncycastle.util.Arrays;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.Holder;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

import java.util.ArrayList;
import java.util.List;

public class TransactionOutpointFilter {
    private final RocksDB db;
    private final ColumnFamilyHandle column;
    private static final NetworkParameters np = new MainNetParams();

    public TransactionOutpointFilter(RocksDB db, ColumnFamilyHandle column) {
        this.db = db;
        this.column = column;
    }

    public void put(Sha256Hash hash, TransactionOutPoint top) throws RocksDBException {
        byte[] key = hash.getBytes();
        byte[] value = top.bitcoinSerialize();

        db.merge(column, key, value);
    }

    public List<TransactionOutPoint> get(Sha256Hash hash) throws RocksDBException {
        byte[] key = hash.getBytes();
        byte[] value = db.get(column, key);

        if (value == null)
            return List.of();

        return deserialize(value);
    }

    public Holder<byte[]> keyMayExist(Sha256Hash hash) {
        Holder<byte[]> holder = new Holder<>();
        byte[] key = hash.getBytes();

        if (db.keyMayExist(column, key, holder))
            return holder;

        return null;
    }

    public static List<TransactionOutPoint> deserialize(byte[] bb) {
        List<TransactionOutPoint> results = new ArrayList<>();
        for (int i = 0; i < bb.length; i += 36)
            results.add(deserializeOne(bb, i, i + 36));
        return results;
    }

    public static TransactionOutPoint deserializeOne(byte[] bb, int from, int to) {
        // 32 byte (hash) | 4 byte (index)
        Sha256Hash hash = Sha256Hash.wrapReversed(Arrays.copyOfRange(bb, from, from + 32));
        int index = ByteConversion.bytes2int(Arrays.reverse(Arrays.copyOfRange(bb, from + 32, to)));

        return new TransactionOutPoint(np, index, hash);
    }
}