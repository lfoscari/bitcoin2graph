package it.unimi.dsi.law.persistence;

import org.bitcoinj.core.Sha256Hash;
import org.bitcoinj.core.TransactionOutPoint;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.ArrayList;
import java.util.List;

public class TransactionOutpointFilter {
    private final RocksDB db;
    private final ColumnFamilyHandle column;

    public TransactionOutpointFilter(RocksDB db, ColumnFamilyHandle column) {
        this.db = db;
        this.column = column;
    }

    public void put(Sha256Hash hash, TransactionOutPoint top) throws RocksDBException {
        byte[] key = hash.getBytes();
        byte[] value =  top.bitcoinSerialize();

        if (get(hash).isEmpty()) {
            value = ByteConversion.concat(value, db.get(column, key));
        }

        db.put(column, key, value);
    }

    public List<TransactionOutPoint> get(Sha256Hash hash) throws RocksDBException {
        byte[] key = hash.getBytes();
        byte[] value = db.get(column, key);

        if (value == null) return List.of();

        try {
            return deserialize(value);
        } catch (IOException | ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    public List<TransactionOutPoint> deserialize(byte[] serialization) throws IOException, ClassNotFoundException {
        ByteArrayInputStream is = new ByteArrayInputStream(serialization);
        ObjectInputStream ois = new ObjectInputStream(is);

        List<TransactionOutPoint> results = new ArrayList<>();
        while (ois.available() > 0)
            results.add((TransactionOutPoint) ois.readObject());

        return results;
    }
}