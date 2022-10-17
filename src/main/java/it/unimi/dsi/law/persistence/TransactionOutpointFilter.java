package it.unimi.dsi.law.persistence;

import com.google.common.primitives.Bytes;
import org.apache.commons.lang3.SerializationUtils;
import org.bitcoinj.core.NetworkParameters;
import org.bitcoinj.core.Sha256Hash;
import org.bitcoinj.core.TransactionOutPoint;
import org.bitcoinj.params.MainNetParams;
import org.bouncycastle.util.Arrays;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

public class TransactionOutpointFilter {
    private final RocksDB db;
    private final ColumnFamilyHandle column;

    public TransactionOutpointFilter(RocksDB db, ColumnFamilyHandle column) {
        this.db = db;
        this.column = column;
    }

    public void put(Sha256Hash hash, TransactionOutPoint top) throws RocksDBException {
        byte[] key = hash.getBytes();

        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        try {
            top.bitcoinSerialize(bos);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        byte[] value = bos.toByteArray();

        if (db.get(column, key) != null)
             value = Bytes.concat(value, db.get(column, key));

        db.put(column, key, value);
    }

    public List<TransactionOutPoint> get(Sha256Hash hash) throws RocksDBException {
        byte[] key = hash.getBytes();
        byte[] value = db.get(column, key);

        if (value == null) return List.of();

        return deserialize(value);
    }

    public List<TransactionOutPoint> deserialize(byte[] bb) {
        List<TransactionOutPoint> results = new ArrayList<>();
        for (int i = 0; i < bb.length; i += 36) {
            byte[] s = Arrays.copyOfRange(bb, i, i + 36);
            results.add(deserializeOne(s));
        }
        return results;
    }

    public TransactionOutPoint deserializeOne(byte[] bb) {
        NetworkParameters np = new MainNetParams();

        // 32 byte (hash) | 4 byte (index)
        Sha256Hash hash = Sha256Hash.wrapReversed(Arrays.copyOf(bb, 32));
        int index = ByteConversion.bytes2int(Arrays.reverse(Arrays.copyOfRange(bb, 32, bb.length)));

        return new TransactionOutPoint(np, index, hash);
    }
}