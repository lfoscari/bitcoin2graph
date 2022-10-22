package it.unimi.dsi.law;

import it.unimi.dsi.fastutil.longs.LongArrayFIFOQueue;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.longs.LongList;
import it.unimi.dsi.law.persistence.*;
import it.unimi.dsi.law.utils.ByteConversion;
import it.unimi.dsi.logging.ProgressLogger;
import it.unimi.dsi.webgraph.BVGraph;
import it.unimi.dsi.webgraph.ScatteredArcsASCIIGraph;
import org.bitcoinj.core.*;
import org.bitcoinj.params.MainNetParams;
import org.bitcoinj.script.Script;
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

import static org.bitcoinj.script.Script.ScriptType.P2PK;
import static org.bitcoinj.script.Script.ScriptType.P2WPKH;

public class Blockchain2ScatteredArcsASCIIGraph implements Iterable<long[]> {
    public final NetworkParameters np;
    public final String blocksDirectory;
    private final ProgressLogger progress;

    public Blockchain2ScatteredArcsASCIIGraph(String blocksDirectory, ProgressLogger progress) {
        this.blocksDirectory = blocksDirectory;
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
            return new CustomBlockchainIterator<long[]>(blocksDirectory, np, progress);
        } catch (RocksDBException e) {
            throw new RuntimeException(e);
        }
    }

    private static class CustomBlockchainIterator<T> implements Iterator<long[]> {
        private final NetworkParameters np;
        private final ProgressLogger progress;

        private final BlockFileLoader bfl;
        private final LongArrayFIFOQueue transactionArcs = new LongArrayFIFOQueue();

        private final PersistenceLayer persistenceLayer = PersistenceLayer.getInstance(Parameters.resources + "bitcoin-parser-db");
        private final AddressConversion addressConversion = persistenceLayer.getAddressConversion();
        private final IncompleteMappings incompleteMappings = persistenceLayer.getIncompleteMappings();
        private final TransactionOutpointFilter topFilter = persistenceLayer.getTransactionOutpointFilter();

        private final long COINBASE_ADDRESS = 0L;

        public CustomBlockchainIterator(String blocksDirectory, NetworkParameters np, ProgressLogger progress) throws RocksDBException {
            this.np = np;
            this.progress = progress;

            List<File> blockFiles = getBlockFiles(blocksDirectory);

            progress.start("First pass to populate mappings");
            BlockFileLoader bflTemp = new BlockFileLoader(np, blockFiles);

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

            this.bfl = new BlockFileLoader(np, blockFiles);
        }

        List<File> getBlockFiles(String directory) {
            FilenameFilter blockFileFilter = (d, s) -> s.toLowerCase().startsWith("blk");
            File[] blockFiles = (new File(directory)).listFiles(blockFileFilter);

            if (blockFiles == null)
                throw new RuntimeException("No blocks found in " + directory + "!");

            return List.of(blockFiles);
        }

        List<Long> outputAddressesToLongs(Transaction t) throws RocksDBException {
            LongList outputs = new LongArrayList();

            for (TransactionOutput to : t.getOutputs()) {
                Script script = null;
                byte[] key;

                try {
                    script = to.getScriptPubKey();

                    // If getToAddress fails I know that the script is not
                    // P2PKH, P2SH, P2PK, P2WH or P2TR, therefore is P2WPKH.
                    // We could handle this case with the catch, but it is
                    // significantly slower.
                    if (script.getScriptType() == null || script.getScriptType().equals(P2WPKH)) {
                        // If the address is malformed in some way try identifying
                        // the receiver via the script, which contains the address
                        // and therefore cannot address someone else, but might not
                        // be unique to the public key.
                        // This cannot introduce wrong arcs, but in the worst case
                        // we get more junk in the database and miss some arcs.
                        key = script.getProgram();
                    } else {
                        Address receiver = script.getToAddress(this.np, true);
                        key = receiver.getHash();
                    }
                } catch (RuntimeException e) {
                    // System.err.println(e.getClass().getCanonicalName() + ": " + e.getMessage());
                    key = script != null ? script.getProgram() : ByteConversion.long2bytes(-1);
                }

                outputs.add(addressConversion.map(key));
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

                    try {
                        List<Long> outputs = outputAddressesToLongs(transaction);
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
