package it.unimi.dsi.law;

import it.unimi.dsi.fastutil.longs.LongArrayFIFOQueue;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.longs.LongList;
import it.unimi.dsi.law.persistence.PersistenceLayer;
import it.unimi.dsi.logging.ProgressLogger;
import org.bitcoinj.core.*;
import org.bitcoinj.core.Transaction;
import org.bitcoinj.script.Script;
import org.bitcoinj.utils.ContextPropagatingThreadFactory;
import org.rocksdb.*;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;

public class CustomBlockchainIterator implements Iterator<long[]>, Iterable<long[]> {
    private final NetworkParameters np;
    private final ProgressLogger progress;
    private final List<File> blockFiles;
    private final AddressConversion addressConversion;
    private PersistenceLayer mappings;

    private ExecutorService executorService;
    private final int numberOfThreads;

    private final LinkedBlockingQueue<Long> transactionArcs = new LinkedBlockingQueue<>();

    public CustomBlockchainIterator(List<File> blockFiles, AddressConversion addressConversion, NetworkParameters np, ProgressLogger progress) throws RocksDBException {
        this.np = np;
        Context c = new Context(this.np);
        Context.propagate(c);

        this.progress = progress;
        this.blockFiles = blockFiles;
        this.addressConversion = addressConversion;

        numberOfThreads = Math.min(this.blockFiles.size(), Runtime.getRuntime().availableProcessors());
        executorService = Executors.newFixedThreadPool(numberOfThreads, new ContextPropagatingThreadFactory("blockchain-parser"));

    }

    public void populateMappings() throws RocksDBException, IOException, ExecutionException, InterruptedException {
        mappings = new PersistenceLayer(Parameters.resources + "bitcoin-db");
        progress.start("Populating mappings on block files with " + numberOfThreads + " threads");

        ExecutorCompletionService<PersistenceLayer> executorCompletionService = new ExecutorCompletionService<>(executorService);
        int factor = this.blockFiles.size() / numberOfThreads;
        for (int i = 0; i < numberOfThreads; i++) {
            List<File> assignedBlockFiles = blockFiles.subList(i * factor, (i + 1) * factor);
            PopulateMappings pm = new PopulateMappings(assignedBlockFiles, transactionArcs, addressConversion, np, progress);
            executorCompletionService.submit(pm);
        }

        for (int i = 0; i < numberOfThreads; i++) {
            PersistenceLayer partialChain = executorCompletionService.take().get();
            mappings.mergeWith(partialChain);
            partialChain.delete();
        }

        progress.stop();
        mappings.close();
    }

    public void completeMappings() throws RocksDBException {
        mappings = new PersistenceLayer(Parameters.resources + "bitcoin-db", true);
        progress.start("Completing mappings with " + numberOfThreads + " threads");

        int factor = this.blockFiles.size() / numberOfThreads;
        for (int i = 0; i < numberOfThreads; i++) {
            List<File> assignedBlockFiles = blockFiles.subList(i * factor, (i + 1) * factor);
            CompleteMappings cm = new CompleteMappings(assignedBlockFiles, transactionArcs, mappings, addressConversion, np, progress);
            executorService.execute(cm);
        }

        progress.stop();
        executorService.shutdown();
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
        Script script = to.getScriptPubKey();

        if (script.getScriptType() == null) {
            // No public keys are contained in this script.
            return null;
        }

        try {
            return script.getToAddress(np, true);
        } catch (IllegalArgumentException e) {
            // Non standard address
            return null;
        }
    }

    @Override
    public boolean hasNext() {
        while (transactionArcs.size() < 2) {
            if (executorService.isTerminated()) {
                mappings.close();
                return false;
            }
        }

        return true;
    }

    @Override
    public long[] next() {
        long sender = transactionArcs.poll();
        long receiver = transactionArcs.poll();
        return new long[] {sender, receiver};
    }

    @Override
    public Iterator<long[]> iterator() {
        return this;
    }
}