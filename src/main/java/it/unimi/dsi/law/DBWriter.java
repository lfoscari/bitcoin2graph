package it.unimi.dsi.law;

import it.unimi.dsi.law.persistence.PersistenceLayer;
import it.unimi.dsi.logging.ProgressLogger;
import org.rocksdb.RocksDBException;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteOptions;

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

	private void digest (WriteBatch wb) throws RocksDBException {
		this.mappings.db.write(new WriteOptions(), wb);
	}

	@Override
	public void run () {
		while (!this.stop) {
			try (WriteBatch wb = this.wbQueue.poll()) {
				if (wb == null) {
					continue;
				}

				this.digest(wb);
			} catch (RocksDBException e) {
				throw new RuntimeException(e);
			}
		}
	}

	public void flush () throws RocksDBException {
		for (WriteBatch writeBatch : this.wbQueue) {
			this.digest(writeBatch);
		}
	}
}
