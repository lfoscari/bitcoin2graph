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
	public volatile boolean stop = false;

	public DBWriter (PersistenceLayer mappings, LinkedBlockingQueue<WriteBatch> wbQueue, ProgressLogger progress) {
		this.mappings = mappings;
		this.wbQueue = wbQueue;
		this.progress = progress;
	}

	@Override
	public void run () {
		while (true) {
			try {
				WriteBatch wb = this.wbQueue.poll();

				if (wb == null && this.stop) {
					this.progress.logger.info("No more writes to perform on the database");
					break;
				}

                if (wb == null) {
                    continue;
                }

				this.mappings.db.write(new WriteOptions(), wb);
				this.progress.logger.info("New write batch completed");
			} catch (RocksDBException e) {
				throw new RuntimeException(e);
			}
		}
	}
}
