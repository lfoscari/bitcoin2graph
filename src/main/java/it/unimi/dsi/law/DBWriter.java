package it.unimi.dsi.law;

import it.unimi.dsi.law.persistence.PersistenceLayer;
import org.bitcoinj.core.NetworkParameters;
import org.rocksdb.RocksDBException;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteOptions;

import java.util.concurrent.LinkedBlockingQueue;

public class DBWriter implements Runnable {
    private final PersistenceLayer mappings;
    private final LinkedBlockingQueue<WriteBatch> wbQueue;

    public DBWriter(PersistenceLayer mappings, LinkedBlockingQueue<WriteBatch> wbQueue) {
        this.mappings = mappings;
        this.wbQueue = wbQueue;
    }

    @Override
    public void run() {
        while (!wbQueue.isEmpty()) {
            try {
                WriteBatch wb = wbQueue.poll();
                this.mappings.db.write(new WriteOptions(), wb);
            } catch (RocksDBException ignored) {}
        }
    }
}
