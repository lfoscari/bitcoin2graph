package it.unimi.dsi.law;

import it.unimi.dsi.fastutil.longs.LongArrayFIFOQueue;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.longs.LongList;
import it.unimi.dsi.law.persistence.AddressConversion;
import it.unimi.dsi.law.persistence.IncompleteMappings;
import it.unimi.dsi.law.persistence.PersistenceLayer;
import it.unimi.dsi.law.persistence.TransactionOutpointFilter;
import it.unimi.dsi.law.utils.ByteConversion;
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

import static org.bitcoinj.script.Script.ScriptType.P2WPKH;
import static org.bitcoinj.utils.BlockFileLoader.getReferenceClientBlockFileList;

public class CustomBlockchainIterator { // <T> implements Iterator<long[]> {
    private final NetworkParameters np;
    private final ProgressLogger progress;
    private final List<File> blockFiles;

    private PersistenceLayer pl;

    // private final BlockFileLoader bfl;
    private final LongArrayFIFOQueue transactionArcs = new LongArrayFIFOQueue();

    private static final long COINBASE_ADDRESS = 0L;

    public CustomBlockchainIterator(List<File> blockFiles, NetworkParameters np, ProgressLogger progress) throws RocksDBException, IOException {
        this.np = np;
        Context c = new Context(this.np);
        Context.propagate(c);

        this.progress = progress;
        this.blockFiles = blockFiles;

        try {
            firstPass();
        } catch (ExecutionException | InterruptedException | TimeoutException e) {
            throw new RuntimeException(e);
        }

        // progress.start("Second pass to complete the mappings");
        // this.bfl = new BlockFileLoader(np, blockFiles);
    }

    private void firstPass() throws RocksDBException, IOException, ExecutionException, InterruptedException, TimeoutException {
        int n = this.blockFiles.size();
        int numberOfThreads = Math.min(n, Runtime.getRuntime().availableProcessors());
        int factor = this.blockFiles.size() / numberOfThreads;

        ExecutorService executorService = Executors.newFixedThreadPool(numberOfThreads, new ContextPropagatingThreadFactory("blockchain-parser"));
        ExecutorCompletionService<PersistenceLayer> executorCompletionService = new ExecutorCompletionService<>(executorService);

        PersistenceLayer fullChain = new PersistenceLayer(Parameters.resources + "bitcoin-db");

        progress.start("Populating mappings on block files");

        for (int worker = 0; worker < numberOfThreads; worker++) {
            List<File> assignedBlockFiles = blockFiles.subList(worker * factor, (worker + 1) * factor);
            PopulateMappings pm = new PopulateMappings(assignedBlockFiles, np, progress);
            executorCompletionService.submit(pm);
        }

        for (int i = 0; i < numberOfThreads; i++) {
            PersistenceLayer partialChain = executorCompletionService.take().get();

            // TODO: merge transaction arcs as well

            fullChain.mergeWith(partialChain);
            partialChain.close();
        }

        fullChain.close();
        executorService.shutdown();

        progress.stop("Done populating mappings");
    }

    public static List<Long> outputAddressesToLongs(Transaction t, AddressConversion addressConversion, NetworkParameters np) throws RocksDBException {
        LongList outputs = new LongArrayList();

        for (TransactionOutput to : t.getOutputs()) {
            byte[] key;

            try {
                Script script = to.getScriptPubKey();
                Script.ScriptType type = script.getScriptType();

                // If getToAddress fails I know that the script is not
                // P2PKH, P2SH, P2PK, P2WH or P2TR, therefore is P2WPKH.
                // We could handle this case with the catch, but it is
                // significantly slower.
                if (type == null || type.equals(P2WPKH)) {
                    // If the address is malformed in some way try identifying
                    // the receiver via the script, which contains the address
                    // and therefore cannot address someone else, but might not
                    // be unique to the public key.
                    // This cannot introduce wrong arcs, but in the worst case
                    // we get more junk in the database and miss some arcs.
                    key = script.getPubKeyHash();
                } else {
                    Address receiver = script.getToAddress(np, true);
                    key = receiver.getHash();
                }

                outputs.add(addressConversion.map(key));
            } catch (RuntimeException e) {
                // System.err.println(e.getClass().getCanonicalName() + ": " + e.getMessage());
                outputs.add(-1);
            }
        }

        return outputs;
    }

    public static void mapTransactionOutputs(Transaction transaction, List<Long> receivers, IncompleteMappings incompleteMappings, TransactionOutpointFilter topFilter) throws RocksDBException {
        for (TransactionInput ti : transaction.getInputs()) {
            TransactionOutPoint top = ti.getOutpoint();

            incompleteMappings.put(top, receivers);
            topFilter.put(top.getHash(), top.getIndex());
        }
    }

    public static void addCoinbaseArcs(List<Long> receivers, LongArrayFIFOQueue transactionArcs) {
        for (long receiver : receivers) {
            transactionArcs.enqueue(COINBASE_ADDRESS);
            transactionArcs.enqueue(receiver);
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