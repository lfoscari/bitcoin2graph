package it.unimi.dsi.law;

import it.unimi.dsi.fastutil.Function;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.longs.LongList;
import it.unimi.dsi.law.persistence.PersistenceLayer;
import it.unimi.dsi.logging.ProgressLogger;
import org.bitcoinj.core.*;
import org.bitcoinj.script.Script;
import org.bitcoinj.script.ScriptException;
import org.bitcoinj.utils.ContextPropagatingThreadFactory;
import org.rocksdb.RocksDBException;
import org.rocksdb.WriteBatch;

import java.io.File;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.*;

public class CustomBlockchainIterator {
	private final NetworkParameters np;
	private final ProgressLogger progress;
	private final AddressConversion addressConversion;
	private PersistenceLayer mappings;

	private final LinkedBlockingQueue<Long[]> transactionArcs;
	private final LinkedBlockingQueue<List<byte[]>> blockQueue;
	private final LinkedBlockingQueue<WriteBatch> wbQueue;

	private final List<File> blockFiles;
	private ExecutorService blockchainParsers;
	private final RocksArcs rocksArcs;

	public CustomBlockchainIterator (List<File> blockFiles, AddressConversion addressConversion, NetworkParameters np, ProgressLogger progress) throws RocksDBException {
		this.np = np;
		this.progress = progress;
		this.addressConversion = addressConversion;

		this.transactionArcs = new LinkedBlockingQueue<>();
		this.blockQueue = new LinkedBlockingQueue<>(Parameters.numberOfThreads / 2);
		this.wbQueue = new LinkedBlockingQueue<>(Parameters.numberOfThreads);

		this.blockFiles = blockFiles;
		this.rocksArcs = new RocksArcs(this.transactionArcs);
	}

	public void populateMappings () throws RocksDBException, InterruptedException, ExecutionException {
		this.progress.start("Populating mappings with " + Parameters.numberOfThreads + " threads");

		this.mappings = new PersistenceLayer(Parameters.resources + "bitcoin-db", false);

		BlockLoader bl = new BlockLoader(this.blockFiles, this.blockQueue, this.progress, this.np);
		Future<?> loaderStatus = Executors.newSingleThreadExecutor(new ContextPropagatingThreadFactory("block-loader")).submit(bl);

		DBWriter dbWriter = new DBWriter(this.mappings, this.wbQueue, this.progress);
		Future<?> writerStatus = Executors.newSingleThreadExecutor(new ContextPropagatingThreadFactory("db-writer")).submit(dbWriter);

		this.blockchainParsers = Executors.newFixedThreadPool(Parameters.numberOfThreads, new ContextPropagatingThreadFactory("populating-mappings"));
		List<Future<?>> pmTasks = new ArrayList<>();

		while (!loaderStatus.isDone()) {

			// Manage the number of active PopulateMappings tasks to avoid thrashing
			if (pmTasks.size() > Parameters.numberOfThreads) {
				pmTasks.removeIf(Future::isDone);
				continue;
			}

			List<byte[]> blocksBytes = this.blockQueue.poll();

            if (blocksBytes == null) {
                continue;
            }

			this.progress.logger.info("New mapping task added");
			PopulateMappings pm = new PopulateMappings(blocksBytes, this.addressConversion, this.transactionArcs, this.mappings.getColumnFamilyHandleList(), this.wbQueue, this.np, this.progress);
			Future<?> pmFuture = this.blockchainParsers.submit(pm);
			pmTasks.add(pmFuture);
		}

		this.blockchainParsers.shutdown();

        for (Future<?> pmFuture : pmTasks) {
            pmFuture.get();
        }

		dbWriter.stop = true;
		writerStatus.get();
		loaderStatus.get();

		this.blockchainParsers.awaitTermination(Long.MAX_VALUE, TimeUnit.SECONDS);

		this.mappings.close();
		this.progress.stop();
	}

	public void completeMappings () throws RocksDBException, InterruptedException, ExecutionException {
		this.progress.start("Completing mappings with " + Parameters.numberOfThreads + " threads");

		this.mappings = new PersistenceLayer(Parameters.resources + "bitcoin-db", true);

		BlockLoader blockLoader = new BlockLoader(this.blockFiles, this.blockQueue, this.progress, this.np);
		Future<?> loaderStatus = Executors.newSingleThreadExecutor(new ContextPropagatingThreadFactory("block-loader")).submit(blockLoader);

		DBWriter dbWriter = new DBWriter(this.mappings, this.wbQueue, this.progress);
		Future<?> writerStatus = Executors.newSingleThreadExecutor(new ContextPropagatingThreadFactory("db-writer")).submit(dbWriter);

		this.blockchainParsers = Executors.newFixedThreadPool(Parameters.numberOfThreads / 2, new ContextPropagatingThreadFactory("completing-mappings"));
		List<Future<?>> cmTasks = new ArrayList<>();

		while (!loaderStatus.isDone()) {

			// Manage the number of active CompleteMappings tasks to avoid thrashing
			if (cmTasks.size() > Parameters.numberOfThreads / 2) {
				cmTasks.removeIf(Future::isDone);
				continue;
			}

			List<byte[]> blocksBytes = this.blockQueue.poll();

            if (blocksBytes == null) {
                continue;
            }

			this.progress.logger.info("New mapping task added");
			CompleteMappings cm = new CompleteMappings(blocksBytes, this.addressConversion, this.transactionArcs, this.mappings, this.np, this.progress);
			Future<?> cmFuture = this.blockchainParsers.submit(cm);
			cmTasks.add(cmFuture);
		}

		this.blockchainParsers.shutdown();

        for (Future<?> cmFuture : cmTasks) {
            cmFuture.get();
        }

		dbWriter.stop = true;
		writerStatus.get();
		loaderStatus.get();

		this.blockchainParsers.awaitTermination(Long.MAX_VALUE, TimeUnit.SECONDS);

		this.mappings.close();
		this.progress.stop();
		this.rocksArcs.flush();
	}

	public static List<Long> outputAddressesToLongs (Transaction t, AddressConversion ac, NetworkParameters np) throws RocksDBException {
		LongList outputs = new LongArrayList();

		for (TransactionOutput to : t.getOutputs()) {
			Address receiver = transactionOutputToAddress(to, np);
			outputs.add(receiver == null ? -1 : ac.map(receiver));
		}

		return outputs;
	}

	public static Address transactionOutputToAddress (TransactionOutput to, NetworkParameters np) {
		try {
			Script script = to.getScriptPubKey();

			if (script.getScriptType() == null) {
				// No public keys are contained in this script.
				return null;
			}

			return script.getToAddress(np, true);
		} catch (IllegalArgumentException | ScriptException e) {
			// Non-standard address
			return null;
		}
	}
}