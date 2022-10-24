package it.unimi.dsi.law;

import it.unimi.dsi.law.utils.ByteConversion;
import org.bitcoinj.utils.BlockFileLoader;

import it.unimi.dsi.fastutil.longs.LongArrayFIFOQueue;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.longs.LongList;
import it.unimi.dsi.law.persistence.*;
import it.unimi.dsi.logging.ProgressLogger;
import it.unimi.dsi.webgraph.BVGraph;
import it.unimi.dsi.webgraph.ScatteredArcsASCIIGraph;
import org.bitcoinj.core.*;
import org.bitcoinj.params.MainNetParams;
import org.bitcoinj.script.Script;
import org.rocksdb.Holder;
import org.rocksdb.RocksDBException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.*;
import java.util.stream.Collectors;

import static org.bitcoinj.script.Script.ScriptType.P2WPKH;
import static org.bitcoinj.utils.BlockFileLoader.getReferenceClientBlockFileList;

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
        ProgressLogger progress = new ProgressLogger(logger, Parameters.logInterval, Parameters.logTimeUnit, "blocks");
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
            return new CustomBlockchainIterator<long[]>(new File(blocksDirectory), np, progress);
        } catch (RocksDBException e) {
            throw new RuntimeException(e);
        }
    }

    private static class CustomBlockchainIterator<T> implements Iterator<long[]> {
        private final NetworkParameters np;
        private final ProgressLogger progress;

        private final BlockFileLoader bfl;
        private final LongArrayFIFOQueue transactionArcs = new LongArrayFIFOQueue();

        private final PersistenceLayer persistenceLayer = PersistenceLayer.getInstance(Parameters.resources + "bitcoin-db");
        private final AddressConversion addressConversion = persistenceLayer.getAddressConversion();
        private final IncompleteMappings incompleteMappings = persistenceLayer.getIncompleteMappings();
        private final TransactionOutpointFilter topFilter = persistenceLayer.getTransactionOutpointFilter();

        private final long COINBASE_ADDRESS = 0L;

        public CustomBlockchainIterator(File blocksDirectory, NetworkParameters np, ProgressLogger progress) throws RocksDBException {
            this.np = np;
            this.progress = progress;

            progress.start("Saving in parsed-blocks.txt the block to parse");
            savedParsedBlocksFilenames(blocksDirectory, "parsed-blocks.txt");
            progress.stop();

            progress.start("First pass to populate mappings");
            BlockFileLoader bflTemp = new BlockFileLoader(np, blocksDirectory);

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

            this.bfl = new BlockFileLoader(np, blocksDirectory);
        }

        void savedParsedBlocksFilenames(File blocksDirectory, String filename) {
            try {
                List<File> fileToParse = getReferenceClientBlockFileList(blocksDirectory);
                File out = new File(Parameters.resources + filename);

                try (OutputStream os = new FileOutputStream(out)) {
                    os.write(fileToParse.stream().map(File::getName).collect(Collectors.joining("\n")).getBytes());
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        List<Long> outputAddressesToLongs(Transaction t) throws RocksDBException {
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
                        Address receiver = script.getToAddress(this.np, true);
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

        private void populateMappings(Transaction transaction, List<Long> receivers) throws RocksDBException {
            for (TransactionInput ti : transaction.getInputs()) {
                TransactionOutPoint top = ti.getOutpoint();

                incompleteMappings.put(top, receivers);
                topFilter.put(top.getHash(), top.getIndex());
            }
        }

        private void addCoinbaseArcs(List<Long> receivers) {
            for (long receiver : receivers) {
                transactionArcs.enqueue(COINBASE_ADDRESS);
                transactionArcs.enqueue(receiver);
            }
        }

        private void completeMappings(Transaction transaction, List<Long> senders) throws RocksDBException {
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
            progress.update();

            long sender = transactionArcs.dequeueLong();
            long receiver = transactionArcs.dequeueLong();
            return new long[] {sender, receiver};
        }
    }
}
