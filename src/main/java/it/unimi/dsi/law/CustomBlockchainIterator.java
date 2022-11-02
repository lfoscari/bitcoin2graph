package it.unimi.dsi.law;

import it.unimi.dsi.fastutil.longs.LongArrayFIFOQueue;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.longs.LongList;
import it.unimi.dsi.law.AddressConversion;
import it.unimi.dsi.law.persistence.IncompleteMappings;
import it.unimi.dsi.law.persistence.PersistenceLayer;
import it.unimi.dsi.law.persistence.TransactionOutpointFilter;
import it.unimi.dsi.logging.ProgressLogger;
import org.bitcoinj.core.*;
import org.bitcoinj.core.Transaction;
import org.bitcoinj.script.Script;
import org.bitcoinj.utils.BlockFileLoader;
import org.bitcoinj.utils.ContextPropagatingThreadFactory;
import org.rocksdb.*;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;

import static org.bitcoinj.script.Script.ScriptType.P2WPKH;
import static org.bitcoinj.utils.BlockFileLoader.getReferenceClientBlockFileList;

public class CustomBlockchainIterator { // <T> implements Iterator<long[]> {
    private final NetworkParameters np;
    private final ProgressLogger progress;
    private final List<File> blockFiles;
    private final AddressConversion addressConversion;

    private PersistenceLayer pl;

    // private final BlockFileLoader bfl;
    private final LongArrayFIFOQueue transactionArcs = new LongArrayFIFOQueue();

    public CustomBlockchainIterator(List<File> blockFiles, AddressConversion addressConversion, NetworkParameters np, ProgressLogger progress) {
        this.np = np;
        Context c = new Context(this.np);
        Context.propagate(c);

        this.progress = progress;
        this.blockFiles = blockFiles;
        this.addressConversion = addressConversion;

        // progress.start("Second pass to complete the mappings");
        // this.bfl = new BlockFileLoader(np, blockFiles);
    }

    public void populateMappings() throws RocksDBException, IOException, ExecutionException, InterruptedException {
        int numberOfThreads = Math.min(this.blockFiles.size(), Runtime.getRuntime().availableProcessors());
        int factor = this.blockFiles.size() / numberOfThreads;

        ExecutorService executorService = Executors.newFixedThreadPool(numberOfThreads, new ContextPropagatingThreadFactory("blockchain-parser"));
        ExecutorCompletionService<PersistenceLayer> executorCompletionService = new ExecutorCompletionService<>(executorService);

        PersistenceLayer fullChain = new PersistenceLayer(Parameters.resources + "bitcoin-db");

        progress.start("Populating mappings on block files with " + numberOfThreads + " threads");

        for (int worker = 0; worker < numberOfThreads; worker++) {
            List<File> assignedBlockFiles = blockFiles.subList(worker * factor, (worker + 1) * factor);
            PopulateMappings pm = new PopulateMappings(assignedBlockFiles, addressConversion, np, progress);
            executorCompletionService.submit(pm);
        }

        for (int i = 0; i < numberOfThreads; i++) {
            PersistenceLayer partialChain = executorCompletionService.take().get();

            // TODO: merge transaction arcs as well

            fullChain.mergeWith(partialChain);
            partialChain.delete();
        }

        fullChain.close();
        executorService.shutdown();

        progress.stop("Done populating mappings");
    }

    public static List<Long> outputAddressesToLongs(Transaction t, AddressConversion ac, NetworkParameters np) throws RocksDBException {
        LongList outputs = new LongArrayList();

        for (TransactionOutput to : t.getOutputs()) {
            Address receiver = transactionOutputToAddress(to, np);
            outputs.add(receiver == null ? -1 : ac.map(receiver));
        }

        return outputs;

        // return t.getOutputs().stream().map(addressConversion::map).collect(Collectors.toList());
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

    /* private void completeMappings(Transaction transaction, List<Long> senders) throws RocksDBException {
        List<Long> tops;
        Sha256Hash txId = transaction.getTxId();
        Holder<byte[]> exists = topFilter.keyMayExist(txId);

        if (exists == null)
            return;

        tops = exists.getValue() != null ?
                ByteConversion.bytes2longList(exists.getValue())
                : topFilter.get(txId);

        if (tops == null)
            return;

        for (long index : tops) {
            TransactionOutPoint top = new TransactionOutPoint(np, index, txId);
            long sender = senders.get((int) index);

            incompleteMappings
                    .get(top)
                    .stream()
                    .filter(Objects::nonNull)
                    .unordered()
                    .distinct() // multigraph -> graph
                    .forEach(receiver -> {
                        transactionArcs.enqueue(sender);
                        transactionArcs.enqueue((long) receiver);
                    });
        }
    }

    @Override
    public boolean hasNext() {
        while (bfl.hasNext()) {

            if (!transactionArcs.isEmpty())
                return true;

            Block block = bfl.next();

            if (!block.hasTransactions())
                continue;

            for (Transaction transaction : block.getTransactions()) {
                if (transaction.isCoinBase())
                    continue;

                try {
                    List<Long> outputs = outputAddressesToLongs(transaction, addressConversion, np);
                    completeMappings(transaction, outputs);
                } catch (RocksDBException e) {
                    throw new RuntimeException(e);
                }
            }
        }

        progress.stop("Done completing the mappings in " + progress.millis() / 1000  + " seconds");

        persistenceLayer.close();
        return false;
    }

    @Override
    public long[] next() {
        progress.update();

        long sender = transactionArcs.dequeueLong();
        long receiver = transactionArcs.dequeueLong();
        return new long[] {sender, receiver};
    } */
}