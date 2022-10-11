package it.unimi.dsi.law;

import it.unimi.dsi.webgraph.BVGraph;
import it.unimi.dsi.webgraph.ScatteredArcsASCIIGraph;
import org.apache.commons.collections4.MultiValuedMap;
import org.apache.commons.collections4.multimap.ArrayListValuedHashMap;
import org.bitcoinj.core.*;
import org.bitcoinj.params.MainNetParams;
import org.bitcoinj.script.ScriptException;
import org.bitcoinj.utils.BlockFileLoader;
import org.bitcoinj.utils.BriefLogFormatter;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

public class Blockchain2ScatteredArcsASCIIGraph implements Iterable<long[]> {
    public final String blockfile;
    public final NetworkParameters np;

    public static void main(String[] args) throws IOException {
        Blockchain2ScatteredArcsASCIIGraph bt = new Blockchain2ScatteredArcsASCIIGraph(Blockchain2ArrayListMutableGraph.defaultLocation + "blk00000.dat");
        ScatteredArcsASCIIGraph graph = new ScatteredArcsASCIIGraph(bt.iterator(), false, false, 1000, null, null);
        BVGraph.store(graph, Blockchain2ArrayListMutableGraph.defaultLocation + "ScatteredArcsASCIIGraph/bitcoin");
        System.out.println("Results saved in " + Blockchain2ArrayListMutableGraph.defaultLocation + "ScatteredArcsASCIIGraph/bitcoin");
    }

    public Blockchain2ScatteredArcsASCIIGraph(String blockfile) {
        this.blockfile = blockfile;
        this.np = new MainNetParams();

        BriefLogFormatter.init();
        new Context(this.np);
    }

    @Override
    public Iterator<long[]> iterator() {
        return new CustomBlockchainIterator<long[]>(blockfile, np);
    }

    private static class CustomBlockchainIterator<T> implements Iterator<long[]> {
        private final BlockFileLoader bfl;
        private final NetworkParameters np;

        private final ArrayDeque<long[]> transactionArcs = new ArrayDeque<>();

        public HashMap<Address, Long> addressConversion = new HashMap<>();
        public static long totalNodes = 0;

        private final MultiValuedMap<TransactionOutPoint, Long> incomplete = new ArrayListValuedHashMap<>();
        private final MultiValuedMap<Sha256Hash, TransactionOutPoint> topMapping = new ArrayListValuedHashMap<>();

        public CustomBlockchainIterator(String blockfile, NetworkParameters np) {
            this.np = np;

            // First pass to populate mappings
            BlockFileLoader bflTemp = new BlockFileLoader(np, List.of(new File(blockfile)));
            for (Block block: bflTemp) {
                if (!block.hasTransactions())
                    continue;

                for (Transaction transaction: block.getTransactions()) {
                    if (transaction.isCoinBase())
                        continue;

                    List<Long> outputs = outputAddressesToLongs(transaction);
                    populateMappings(transaction, outputs);
                }
            }

            this.bfl = new BlockFileLoader(np, List.of(new File(blockfile)));;
        }

        Long addressToLong(Address a) {
            if (addressConversion.containsKey(a)) {
                return addressConversion.get(a);
            }

            addressConversion.put(a, totalNodes);
            return totalNodes++;
        }

        List<Long> outputAddressesToLongs(Transaction t) {
            List<Long> outputs = new ArrayList<>();

            for (TransactionOutput to: t.getOutputs()) {
                try {
                    Address receiver = to.getScriptPubKey().getToAddress(this.np, true);
                    Long receiverLong = addressToLong(receiver);
                    outputs.add(receiverLong);
                } catch (ScriptException e) {
                    outputs.add(null); // Don't mess up the indexing
                    System.out.println(e.getMessage() + " at " + t.getTxId());
                }
            }

            return outputs;
        }

        private void populateMappings(Transaction transaction, List<Long> receivers) {
            for (TransactionInput ti: transaction.getInputs()) {
                TransactionOutPoint top = ti.getOutpoint();

                incomplete.putAll(top, receivers);
                topMapping.put(top.getHash(), top);
            }
        }

        private void completeMappings(Transaction transaction, List<Long> senders) {
            Sha256Hash txId = transaction.getTxId();

            if (!topMapping.containsKey(txId))
                return;

            for (TransactionOutPoint top: topMapping.get(txId)) {
                int index = (int) top.getIndex();
                List<Long> dedupReceivers = incomplete
                        .get(top)
                        .stream()
                        .filter(Objects::nonNull)
                        .sorted()
                        .distinct()
                        .collect(Collectors.toList());

                for (Long receiver: dedupReceivers) {
                    Long sender = senders.get(index);
                    transactionArcs.add(new long[] {sender, receiver});
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

                for (Transaction transaction: block.getTransactions()) {
                    if (transaction.isCoinBase())
                        continue;

                    List<Long> outputs = outputAddressesToLongs(transaction);
                    completeMappings(transaction, outputs);
                }
            }

            return false;
        }

        @Override
        public long[] next() {
            return transactionArcs.pop();
        }
    }
}
