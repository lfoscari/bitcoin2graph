package it.unimi.dsi.law;

import it.unimi.dsi.law.persistence.PersistenceLayer;
import it.unimi.dsi.logging.ProgressLogger;
import org.bitcoinj.core.NetworkParameters;
import org.rocksdb.RocksDBException;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteOptions;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class DBWriter implements Runnable {
    private final PersistenceLayer mappings;
    private final LinkedBlockingQueue<WriteBatch> wbQueue;
    private final ProgressLogger progress;
    private final WriteBatch stop;

    public DBWriter(PersistenceLayer mappings, LinkedBlockingQueue<WriteBatch> wbQueue, WriteBatch stop, ProgressLogger progress) {
        this.mappings = mappings;
        this.wbQueue = wbQueue;
        this.progress = progress;
        this.stop = stop;
    }

    @Override
    public void run() {
        while (true) {
            WriteBatch wb = wbQueue.poll();

            if (wb == null)
                Thread.yield();

            if (stop.equals(wb))
                return;

            try {
                this.mappings.db.write(new WriteOptions(), wb);
                this.progress.logger.info("New write batch completed");
            } catch (RocksDBException ignored) {}

            System.gc();
        }
    }
}
