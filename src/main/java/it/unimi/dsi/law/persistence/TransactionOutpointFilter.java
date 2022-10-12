package it.unimi.dsi.law.persistence;

import org.rocksdb.RocksDB;

public class TransactionOutpointFilter {
    private final RocksDB db;

    public TransactionOutpointFilter(RocksDB db) {
        this.db = db;
    }
}