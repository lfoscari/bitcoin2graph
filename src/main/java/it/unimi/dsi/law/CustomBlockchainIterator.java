package it.unimi.dsi.law;

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
import org.rocksdb.WriteOptions;
import org.rocksdb.util.SizeUnit;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.*;

public class CustomBlockchainIterator implements Iterator<long[]>, Iterable<long[]> {
    private final NetworkParameters np;
    private final ProgressLogger progress;
    private final AddressConversion addressConversion;
    private PersistenceLayer mappings;

    private final LinkedBlockingQueue<Long> transactionArcs;
    private final LinkedBlockingQueue<List<byte[]>> blockQueue;
    private final LinkedBlockingQueue<WriteBatch> wbQueue;

    private final int numberOfThreads;
    private final List<File> blockFiles;
    private ExecutorService blockchainParsers;
    private ExecutorService diskHandlers;

    public CustomBlockchainIterator(List<File> blockFiles, AddressConversion addressConversion, NetworkParameters np, ProgressLogger progress) {
        this.np = np;
        this.progress = progress;
        this.addressConversion = addressConversion;

        this.transactionArcs = new LinkedBlockingQueue<>();
        this.blockQueue = new LinkedBlockingQueue<>();
        this.wbQueue = new LinkedBlockingQueue<>();

        this.numberOfThreads = Runtime.getRuntime().availableProcessors() - 1;
        this.blockFiles = blockFiles;
    }

    public void populateMappings() throws RocksDBException, InterruptedException {
        progress.start("Populating mappings with " + numberOfThreads + " threads");

        this.mappings = new PersistenceLayer(Parameters.resources + "bitcoin-db", false);

        this.blockchainParsers = Executors.newFixedThreadPool(Parameters.numberOfThreads, new ContextPropagatingThreadFactory("blockchain-parser"));

        this.diskHandlers = Executors.newFixedThreadPool(2, new ContextPropagatingThreadFactory("disk-handler"));
        Future<?> loaderStatus = this.diskHandlers.submit(new BlockLoader(this.blockFiles, blockQueue, np));
        Future<?> writerStatus = this.diskHandlers.submit(new DBWriter(this.mappings, wbQueue));
        diskHandlers.shutdown();

        while (!loaderStatus.isDone()) {
            List<byte[]> blocksBytes = blockQueue.take();
            PopulateMappings pm = new PopulateMappings(blocksBytes, addressConversion, transactionArcs, mappings.getColumnFamilyHandleList(), wbQueue, np, progress);
            blockchainParsers.execute(pm);
        }
        blockchainParsers.shutdown();
        blockchainParsers.awaitTermination(Long.MAX_VALUE, TimeUnit.SECONDS);

        // How to gracefully stop the DBWriter?

        progress.stop();
    }

    public void completeMappings() throws RocksDBException, InterruptedException {
        progress.start("Completing mappings with " + numberOfThreads + " threads");

        this.mappings = new PersistenceLayer(Parameters.resources + "bitcoin-db", true);

        this.blockchainParsers = Executors.newFixedThreadPool(numberOfThreads, new ContextPropagatingThreadFactory("blockchain-parser"));

        this.diskHandlers = Executors.newFixedThreadPool(2, new ContextPropagatingThreadFactory("disk-handler"));
        Future<?> loaderStatus = this.diskHandlers.submit(new BlockLoader(this.blockFiles, blockQueue, np));
        diskHandlers.shutdown();

        while (!loaderStatus.isDone()) {
            List<byte[]> block = blockQueue.take();
            CompleteMappings cm = new CompleteMappings(block, addressConversion, transactionArcs, mappings, np, progress);
            blockchainParsers.execute(cm);
        }

        blockchainParsers.shutdown();
    }

    @Override
    public boolean hasNext() {
        while (transactionArcs.size() < 2) {
            if (blockchainParsers.isTerminated()) {
                this.close();
                progress.done();
                return false;
            }
        }

        return true;
    }

    @Override
    public long[] next() {
        long sender = transactionArcs.poll();
        long receiver = transactionArcs.poll();
        return new long[]{sender, receiver};
    }

    @Override
    public Iterator<long[]> iterator() {
        return this;
    }

    public void close() {
        this.addressConversion.close();
        this.mappings.close();
    }

    public static List<Long> outputAddressesToLongs(Transaction t, AddressConversion ac, NetworkParameters np) throws RocksDBException {
        LongList outputs = new LongArrayList();

        for (TransactionOutput to : t.getOutputs()) {
            Address receiver = transactionOutputToAddress(to, np);
            outputs.add(receiver == null ? -1 : ac.map(receiver));
        }

        return outputs;
    }

    public static Address transactionOutputToAddress(TransactionOutput to, NetworkParameters np) {
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