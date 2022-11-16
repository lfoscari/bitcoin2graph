package it.unimi.dsi.law;

import it.unimi.dsi.law.persistence.PersistenceLayer;
import it.unimi.dsi.logging.ProgressLogger;
import org.bitcoinj.core.NetworkParameters;
import org.bitcoinj.utils.ContextPropagatingThreadFactory;
import org.rocksdb.RocksDBException;
import org.rocksdb.WriteBatch;

import java.io.Closeable;
import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

public class CustomBlockchainIterator implements Closeable {
	private final NetworkParameters np;
	private final ProgressLogger progress;

	private final LinkedBlockingQueue<List<byte[]>> blockQueue;
	private final LinkedBlockingQueue<WriteBatch> wbQueue;

	private final PersistenceLayer mappings;

	private final BlockLoader blockLoader;
	private final DBWriter dbWriter;


	public CustomBlockchainIterator (List<File> blockFiles, NetworkParameters np, ProgressLogger progress) throws RocksDBException {
		this.np = np;
		this.progress = progress;

		this.blockQueue = new LinkedBlockingQueue<>(Parameters.numberOfThreads / 2);
		this.wbQueue = new LinkedBlockingQueue<>();

		this.mappings = new PersistenceLayer(Parameters.resources + "bitcoin");

		this.blockLoader = new BlockLoader(blockFiles, this.blockQueue, this.progress, this.np);
		Executors.newSingleThreadExecutor(new ContextPropagatingThreadFactory("block-loader")).submit(this.blockLoader);

		this.dbWriter = new DBWriter(this.mappings, this.wbQueue, this.progress);
		Executors.newSingleThreadExecutor(new ContextPropagatingThreadFactory("db-writer")).submit(this.dbWriter);
	}

	public void populateMappings () throws RocksDBException, InterruptedException, ExecutionException {
		this.progress.start("Populating mappings with " + Parameters.numberOfThreads + " threads");

		ExecutorService blockchainParsers = Executors.newFixedThreadPool(Parameters.numberOfThreads, new ContextPropagatingThreadFactory("blockchain-parsers"));

		List<Future<?>> pmTasks = new ArrayList<>();
		while (this.blockLoader.hasNext()) {
			// Manage the number of active PopulateMappings tasks to avoid thrashing
			if (pmTasks.size() > Parameters.numberOfThreads) {
				pmTasks.removeIf(Future::isDone);
				continue;
			}

			List<byte[]> blocksBytes = this.blockQueue.poll();

			if (blocksBytes == null) {
				continue;
			}

			PopulateMappings pm = new PopulateMappings(blocksBytes, this.mappings.getColumnFamilyHandleList(), this.wbQueue, this.np, this.progress);
			Future<?> pmFuture = blockchainParsers.submit(pm);
			pmTasks.add(pmFuture);
		}

		blockchainParsers.shutdown();
		blockchainParsers.awaitTermination(Long.MAX_VALUE, TimeUnit.SECONDS);

		for (Future<?> pmFuture : pmTasks) {
			pmFuture.get();
		}

		this.dbWriter.flush();
		this.progress.stop();
	}

	public void completeMappings () throws RocksDBException {
		this.progress.start("Completing mappings");
		new CompleteMappings(this.mappings, this.wbQueue, this.progress);
		this.progress.stop();
	}

	@Override
	public void close () {
		try {
			this.dbWriter.flush();
			this.dbWriter.stop = true;
			this.mappings.close();
		} catch (RocksDBException e) {
			throw new RuntimeException(e);
		}
	}
}