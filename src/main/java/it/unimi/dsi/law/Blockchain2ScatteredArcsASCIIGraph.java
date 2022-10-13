package it.unimi.dsi.law;

import it.unimi.dsi.law.persistence.AddressConversion;
import it.unimi.dsi.law.persistence.IncompleteMappings;
import it.unimi.dsi.law.persistence.PersistenceLayer;
import it.unimi.dsi.webgraph.BVGraph;
import it.unimi.dsi.webgraph.ScatteredArcsASCIIGraph;
import org.apache.commons.collections4.MultiValuedMap;
import org.apache.commons.collections4.multimap.HashSetValuedHashMap;
import org.bitcoinj.core.*;
import org.bitcoinj.params.MainNetParams;
import org.bitcoinj.script.ScriptException;
import org.bitcoinj.utils.BlockFileLoader;
import org.bitcoinj.utils.BriefLogFormatter;
import org.rocksdb.RocksDBException;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

public class Blockchain2ScatteredArcsASCIIGraph implements Iterable<long[]> {
    public final NetworkParameters np;
    public final String blockfilePath;

    public Blockchain2ScatteredArcsASCIIGraph(String blockfilePath) {
        this.blockfilePath = blockfilePath;
        this.np = new MainNetParams();

        BriefLogFormatter.init();
        new Context(this.np);
    }

    public static void main(String[] args) throws IOException {
        Blockchain2ScatteredArcsASCIIGraph bt = new Blockchain2ScatteredArcsASCIIGraph(Parameters.resources + Parameters.blockfile);
        ScatteredArcsASCIIGraph graph = new ScatteredArcsASCIIGraph(bt.iterator(), false, false, 1000, null, null);
        BVGraph.store(graph, Parameters.resources + "ScatteredArcsASCIIGraph/" + Parameters.basename);
        System.out.println("Results saved in " + Parameters.resources + "ScatteredArcsASCIIGraph/" + Parameters.basename);
    }

    @Override
    public Iterator<long[]> iterator() {
        try {
            return new CustomBlockchainIterator<long[]>(blockfilePath, np);
        } catch (RocksDBException e) {
            throw new RuntimeException(e);
        }
    }

    private static class CustomBlockchainIterator<T> implements Iterator<long[]> {
        private final BlockFileLoader bfl;
        private final NetworkParameters np;
        private final ArrayDeque<long[]> transactionArcs = new ArrayDeque<>();

        private final PersistenceLayer persistenceLayer = PersistenceLayer.getInstance();
        private final AddressConversion addressConversion = persistenceLayer.getAddressConversion();
        // private final IncompleteMappings incompleteMappings = persistenceLayer.getIncompleteMappings();
        private final MultiValuedMap<TransactionOutPoint, Long> incompleteMappings = new HashSetValuedHashMap<>();
        private final MultiValuedMap<Sha256Hash, TransactionOutPoint> topMapping = new HashSetValuedHashMap<>();

        public CustomBlockchainIterator(String blockfilePath, NetworkParameters np) throws RocksDBException {
            this.np = np;

            // First pass to populate mappings
            BlockFileLoader bflTemp = new BlockFileLoader(np, List.of(new File(blockfilePath)));
            for (Block block : bflTemp) {
                if (!block.hasTransactions())
                    continue;

                for (Transaction transaction : block.getTransactions()) {
                    if (transaction.isCoinBase())
                        continue;

                    List<Long> outputs = outputAddressesToLongs(transaction);
                    populateMappings(transaction, outputs);
                }
            }

            this.bfl = new BlockFileLoader(np, List.of(new File(blockfilePath)));
        }

        List<Long> outputAddressesToLongs(Transaction t) {
            List<Long> outputs = new ArrayList<>();

            for (TransactionOutput to : t.getOutputs()) {
                try {
                    Address receiver = to.getScriptPubKey().getToAddress(this.np, true);
                    Long receiverLong = addressConversion.mapAddress(receiver);
                    outputs.add(receiverLong);
                } catch (ScriptException e) {
                    outputs.add(null); // Don't mess up the indexing
                    System.out.println(e.getMessage() + " at " + t.getTxId());
                } catch (RocksDBException e) {
                    throw new RuntimeException(e);
                }
            }

            return outputs;
        }

        private void populateMappings(Transaction transaction, List<Long> receivers) {
            for (TransactionInput ti : transaction.getInputs()) {
                TransactionOutPoint top = ti.getOutpoint();

                incompleteMappings.putAll(top, receivers);
                topMapping.put(top.getHash(), top);
            }
        }

        private void completeMappings(Transaction transaction, List<Long> senders) {
            Sha256Hash txId = transaction.getTxId();

            if (!topMapping.containsKey(txId))
                return;

            for (TransactionOutPoint top : topMapping.remove(txId)) {
                int index = (int) top.getIndex();
                List<Long> dedupReceivers = incompleteMappings.get(top)
                        .stream()
                        .filter(Objects::nonNull)
                        .sorted()
                        .distinct() // the HashSetValuedHashMap ensures this
                        .collect(Collectors.toList());

                for (Long receiver : dedupReceivers) {
                    Long sender = senders.get(index);
                    transactionArcs.add(new long[]{sender, receiver});
                }
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

                    List<Long> outputs = outputAddressesToLongs(transaction);
                    completeMappings(transaction, outputs);
                }
            }

            persistenceLayer.close();
            return false;
        }

        @Override
        public long[] next() {
            return transactionArcs.pop();
        }
    }
}
