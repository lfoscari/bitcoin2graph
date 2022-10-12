package it.unimi.dsi.law.persistence;

import org.rocksdb.RocksDB;

public class IncompleteMappings {
    private final RocksDB db;

    public IncompleteMappings(RocksDB db) {
        this.db = db;
    }
}