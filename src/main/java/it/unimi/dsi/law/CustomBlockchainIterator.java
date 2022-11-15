package it.unimi.dsi.law;

import com.google.common.primitives.Bytes;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.longs.LongList;
import it.unimi.dsi.law.persistence.PersistenceLayer;
import it.unimi.dsi.law.utils.ByteConversion;
import it.unimi.dsi.logging.ProgressLogger;
import org.bitcoinj.core.Address;
import org.bitcoinj.core.NetworkParameters;
import org.bitcoinj.core.Transaction;
import org.bitcoinj.core.TransactionOutput;
import org.bitcoinj.script.Script;
import org.bitcoinj.script.ScriptException;
import org.bitcoinj.utils.ContextPropagatingThreadFactory;
import org.rocksdb.RocksDBException;
import org.rocksdb.WriteBatch;

import java.io.Closeable;
import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.*;

public class CustomBlockchainIterator implements Closeable {
	private final NetworkParameters np;
	private final ProgressLogger progress;
	private final AddressConversion addressConversion;

	private final LinkedBlockingQueue<Long[]> transactionArcs;
	private final LinkedBlockingQueue<List<byte[]>> blockQueue;
	private final LinkedBlockingQueue<WriteBatch> wbQueue;
	private final ExecutorService blockchainParsers;

	private final RocksArcs rocksArcs;
	private final PersistenceLayer mappings;

	private final BlockLoader blockLoader;
	private final DBWriter dbWriter;

	public static final byte[] UNKNOWN = Bytes.ensureCapacity(ByteConversion.long2bytes(-1), 20, 0);

	public CustomBlockchainIterator (List<File> blockFiles, AddressConversion addressConversion, NetworkParameters np, ProgressLogger progress) throws RocksDBException {
		this.np = np;
		this.progress = progress;
		this.addressConversion = addressConversion;

		this.transactionArcs = new LinkedBlockingQueue<>();
		this.blockQueue = new LinkedBlockingQueue<>(Parameters.numberOfThreads / 2);
		this.wbQueue = new LinkedBlockingQueue<>(Parameters.numberOfThreads);
		this.blockchainParsers = Executors.newFixedThreadPool(Parameters.numberOfThreads, new ContextPropagatingThreadFactory("blockchain-parsers"));

		this.rocksArcs = new RocksArcs(this.transactionArcs);
		this.mappings = new PersistenceLayer(Parameters.resources + "bitcoin-db");

		this.blockLoader = new BlockLoader(blockFiles, this.blockQueue, this.progress, this.np);
		Executors.newSingleThreadExecutor(new ContextPropagatingThreadFactory("block-loader")).submit(this.blockLoader);

		this.dbWriter = new DBWriter(this.mappings, this.wbQueue, this.progress);
		Executors.newSingleThreadExecutor(new ContextPropagatingThreadFactory("db-writer")).submit(this.dbWriter);
	}

	public void populateMappings () throws RocksDBException, InterruptedException, ExecutionException {
		this.progress.start("Populating mappings with " + Parameters.numberOfThreads + " threads");

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

			this.progress.logger.info("New mapping task added");
			PopulateMappings pm = new PopulateMappings(blocksBytes, this.addressConversion, this.transactionArcs, this.mappings.getColumnFamilyHandleList(), this.wbQueue, this.np, this.progress);
			Future<?> pmFuture = this.blockchainParsers.submit(pm);
			pmTasks.add(pmFuture);
		}

		for (Future<?> pmFuture : pmTasks) {
			pmFuture.get();
		}

		this.dbWriter.flush();
		this.blockLoader.reset();

		this.progress.stop();
	}

	public void completeMappings () throws RocksDBException, InterruptedException, ExecutionException {
		this.progress.start("Completing mappings with " + Parameters.numberOfThreads + " threads");

		Executors.newSingleThreadExecutor(new ContextPropagatingThreadFactory("block-loader")).submit(this.blockLoader);

		List<Future<?>> cmTasks = new ArrayList<>();
		while (this.blockLoader.hasNext()) {
			if (cmTasks.size() > Parameters.numberOfThreads) {
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

		boolean ignored = this.blockchainParsers.awaitTermination(Long.MAX_VALUE, TimeUnit.SECONDS);

		for (Future<?> cmFuture : cmTasks) {
			cmFuture.get();
		}

		this.progress.stop();
	}

	public static List<byte[]> outputAddresses (Transaction t, NetworkParameters np) {
		return t.getOutputs().stream().map(to -> transactionOutputToAddress(to, np)).toList();
	}

	public static byte[] transactionOutputToAddress (TransactionOutput to, NetworkParameters np) {
		try {
			Script script = to.getScriptPubKey();

			if (script.getScriptType() == null) {
				// No public keys are contained in this script.
				return UNKNOWN;
			}

			return script.getToAddress(np, true).getHash();
		} catch (IllegalArgumentException | ScriptException e) {
			// Non-standard address
			return UNKNOWN;
		}
	}

	@Override
	public void close () {
		try {
			this.dbWriter.stop = true;
			this.blockLoader.stop = true;
			this.blockchainParsers.awaitTermination(Long.MAX_VALUE, TimeUnit.SECONDS);
			this.dbWriter.flush();
			this.mappings.close();
			this.rocksArcs.close();
		} catch (RocksDBException | InterruptedException e) {
			throw new RuntimeException(e);
		}
	}
}