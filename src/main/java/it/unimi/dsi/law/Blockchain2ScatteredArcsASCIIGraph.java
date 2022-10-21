package it.unimi.dsi.law;

import it.unimi.dsi.fastutil.longs.LongArrayFIFOQueue;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.longs.LongList;
import it.unimi.dsi.fastutil.longs.LongLongImmutablePair;
import it.unimi.dsi.fastutil.objects.ObjectArrayFIFOQueue;
import it.unimi.dsi.law.persistence.AddressConversion;
import it.unimi.dsi.law.persistence.IncompleteMappings;
import it.unimi.dsi.law.persistence.PersistenceLayer;
import it.unimi.dsi.law.persistence.TransactionOutpointFilter;
import it.unimi.dsi.logging.ProgressLogger;
import it.unimi.dsi.webgraph.BVGraph;
import it.unimi.dsi.webgraph.ScatteredArcsASCIIGraph;
import org.bitcoinj.core.*;
import org.bitcoinj.params.MainNetParams;
import org.bitcoinj.script.ScriptException;
import org.bitcoinj.utils.BlockFileLoader;
import org.rocksdb.RocksDBException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.TimeUnit;

public class Blockchain2ScatteredArcsASCIIGraph implements Iterable<long[]> {
    public final NetworkParameters np;
    public final String blockfilePath;
    private final ProgressLogger progress;

    public Blockchain2ScatteredArcsASCIIGraph(String blockfilePath, ProgressLogger progress) {
        this.blockfilePath = blockfilePath;
        this.np = new MainNetParams();
        this.progress = progress;

        new Context(this.np);
    }

    public static void main(String[] args) throws IOException {
        (new File(Parameters.resources + "ScatteredArcsASCIIGraph/")).mkdir();

        Logger logger = LoggerFactory.getLogger(Blockchain2ScatteredArcsASCIIGraph.class);
        ProgressLogger progress = new ProgressLogger(logger, Parameters.logInterval, TimeUnit.MINUTES, "blocks");
        progress.displayFreeMemory = true;
        progress.displayLocalSpeed = true;

        Blockchain2ScatteredArcsASCIIGraph bt = new Blockchain2ScatteredArcsASCIIGraph(Parameters.resources, progress);
        ScatteredArcsASCIIGraph graph = new ScatteredArcsASCIIGraph(bt.iterator(), false, false, 10000, null, progress);
        BVGraph.store(graph, Parameters.resources + "ScatteredArcsASCIIGraph/" + Parameters.basename, progress);

        progress.stop("Results saved in " + Parameters.resources + "ScatteredArcsASCIIGraph/" + Parameters.basename);
        progress.done();
    }

    @Override
    public Iterator<long[]> iterator() {
        try {
            return new CustomBlockchainIterator<long[]>(blockfilePath, np, progress);
        } catch (RocksDBException e) {
            throw new RuntimeException(e);
        }
    }

    private static class CustomBlockchainIterator<T> implements Iterator<long[]> {
        private final NetworkParameters np;
        private final ProgressLogger progress;

        private final BlockFileLoader bfl;
        private final LongArrayFIFOQueue transactionArcs = new LongArrayFIFOQueue();

        private final PersistenceLayer persistenceLayer = PersistenceLayer.getInstance("/tmp/bitcoin");
        private final AddressConversion addressConversion = persistenceLayer.getAddressConversion();
        private final IncompleteMappings incompleteMappings = persistenceLayer.getIncompleteMappings();
        private final TransactionOutpointFilter topFilter = persistenceLayer.getTransactionOutpointFilter();

        private final long COINBASE_ADDRESS = 0L;

        public CustomBlockchainIterator(String blockfilesDirectory, NetworkParameters np, ProgressLogger progress) throws RocksDBException {
            this.np = np;
            this.progress = progress;

            FilenameFilter blockFileFilter = (d, s) -> s.toLowerCase().endsWith(".dat");
            File[] blockFiles = (new File(blockfilesDirectory)).listFiles(blockFileFilter);

            if (blockFiles == null)
                throw new RuntimeException("No blocks found in " + blockfilesDirectory + "!");

            progress.start("First pass to populate mappings");

            File blockchainDirectory = new File(blockfilesDirectory);
            BlockFileLoader bflTemp = new BlockFileLoader(np, List.of(blockFiles));

            for (Block block : bflTemp) {
                progress.update();

                if (!block.hasTransactions())
                    continue;

                for (Transaction transaction : block.getTransactions()) {
                    List<Long> outputs = outputAddressesToLongs(transaction);

                    if (transaction.isCoinBase()) {
                        addCoinbaseArcs(outputs);
                    } else {
                        populateMappings(transaction, outputs);
                    }
                }
            }

            progress.stop("Done populating the mappings of " + progress.count + " total blocks in " + progress.millis() / 1000  + " seconds");
            progress.start("Second pass to complete the mappings");

            this.bfl = new BlockFileLoader(np, blockchainDirectory);
        }

        List<Long> outputAddressesToLongs(Transaction t) {
            LongList outputs = new LongArrayList();

            for (TransactionOutput to : t.getOutputs()) {
                try {
                    Address receiver = to.getScriptPubKey().getToAddress(this.np, true);
                    outputs.add(addressConversion.mapAddress(receiver));
                } catch (ScriptException e) {
                    outputs.add(-1L); // Don't mess up the indexing, note that this adds the node -1
                    // System.out.println(e.getMessage() + " at " + t.getTxId());
                } catch (RocksDBException e) {
                    throw new RuntimeException(e);
                }
            }

            return outputs;
        }

        private void populateMappings(Transaction transaction, List<Long> receivers) throws RocksDBException {
            for (TransactionInput ti : transaction.getInputs()) {
                TransactionOutPoint top = ti.getOutpoint();

                incompleteMappings.put(top, receivers);
                topFilter.put(top.getHash(), top);
            }
        }

        private void addCoinbaseArcs(List<Long> receivers) {
            for (long receiver : receivers) {
                transactionArcs.enqueue(COINBASE_ADDRESS);
                transactionArcs.enqueue(receiver);
            }
        }

        private void completeMappings(Transaction transaction, List<Long> senders) throws RocksDBException {
            Sha256Hash txId = transaction.getTxId();

            if (!topFilter.containsKeys(txId))
                return;

            for (TransactionOutPoint top : topFilter.get(txId)) {
                int index = (int) top.getIndex();
                long sender = senders.get(index);

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
                progress.update();

                if (!transactionArcs.isEmpty())
                    return true;

                Block block = bfl.next();

                if (!block.hasTransactions())
                    continue;

                for (Transaction transaction : block.getTransactions()) {
                    if (transaction.isCoinBase())
                        continue;

                    List<Long> outputs = outputAddressesToLongs(transaction);
                    try {
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
            long sender = transactionArcs.dequeueLong();
            long receiver = transactionArcs.dequeueLong();
            return new long[] {sender, receiver};
        }
    }
}
