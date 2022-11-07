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
    private final LinkedBlockingQueue<WriteBatch> dbUpdates;

    private final int numberOfThreads;
    private final List<File> blockFiles;
    private ExecutorService executorService;

    public CustomBlockchainIterator(List<File> blockFiles, AddressConversion addressConversion, NetworkParameters np, ProgressLogger progress) {
        this.np = np;
        this.progress = progress;
        this.addressConversion = addressConversion;

        this.transactionArcs = new LinkedBlockingQueue<>();
        this.blockQueue = new LinkedBlockingQueue<>();
        this.dbUpdates = new LinkedBlockingQueue<>();

        this.numberOfThreads = Runtime.getRuntime().availableProcessors() - 1;
        this.blockFiles = blockFiles;
    }

    public void populateMappings() throws RocksDBException, ExecutionException, InterruptedException {
        progress.start("Populating mappings");

        this.mappings = new PersistenceLayer(Parameters.resources + "bitcoin-db", false);

        this.executorService = Executors.newFixedThreadPool(numberOfThreads, new ContextPropagatingThreadFactory("blockchain-parser"));
        ExecutorCompletionService<WriteBatch> executorCompletionService = new ExecutorCompletionService<>(executorService);

        Future<?> loaderStatus = this.executorService.submit(new BlockLoader(this.blockFiles, blockQueue, np));
        // Future<?> writerStatus = this.executorService.submit(new DBWriter(this.mappings, wbQueue, np));

        while (!loaderStatus.isDone()) {
            List<byte[]> blocksBytes = blockQueue.take();
            PopulateMappings pm = new PopulateMappings(blocksBytes, addressConversion, transactionArcs, mappings.getColumnFamilyHandleList(), np, progress);
            executorCompletionService.submit(pm);
        }

        executorService.shutdown();

        while (!executorService.isTerminated()) {
            WriteBatch wb = executorCompletionService.take().get();
            this.mappings.db.write(new WriteOptions(), wb);
        }

        progress.stop();
    }

    public void completeMappings() throws RocksDBException, IOException, ExecutionException, InterruptedException {
        progress.start("Completing mappings");

        this.mappings = new PersistenceLayer(Parameters.resources + "bitcoin-db", true);

        this.executorService = Executors.newFixedThreadPool(numberOfThreads, new ContextPropagatingThreadFactory("blockchain-parser"));
        Future<?> loaderStatus = this.executorService.submit(new BlockLoader(this.blockFiles, blockQueue, np));

        while (!loaderStatus.isDone()) {
            List<byte[]> block = blockQueue.take();
            CompleteMappings cm = new CompleteMappings(block, addressConversion, transactionArcs, mappings, np, progress);
            executorService.execute(cm);
        }

        executorService.shutdown();
    }

    @Override
    public boolean hasNext() {
        while (transactionArcs.size() < 2) {
            if (executorService.isTerminated()) {
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