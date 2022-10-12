package it.unimi.dsi.law.persistence;

import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDB;

public class TransactionOutpointFilter {
    private final RocksDB db;
    private final ColumnFamilyHandle column;

    public TransactionOutpointFilter(RocksDB db, ColumnFamilyHandle column) {
        this.db = db;
        this.column = column;
    }
}