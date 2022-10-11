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

public class BlockchainIterable implements Iterable<long[]> {
    public final String blockfile;
    public final NetworkParameters np;

    public static void main(String[] args) throws IOException {
        BlockchainIterable bt = new BlockchainIterable(Blockchain2Webgraph.defaultLocation + "blk00000.dat");
        ScatteredArcsASCIIGraph graph = new ScatteredArcsASCIIGraph(bt.iterator(), false, false, 1000, null, null);
        BVGraph.store(graph, Blockchain2Webgraph.defaultLocation + "webgraph2/bitcoin");
        System.out.println("Results saved in " + Blockchain2Webgraph.defaultLocation + "webgraph2/bitcoin");
    }

    public BlockchainIterable(String blockfile) {
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
            this.bfl = new BlockFileLoader(np, List.of(new File(blockfile)));
        }

        Long addressToLong(Address a) {
            if (addressConversion.containsKey(a)) {
                return addressConversion.get(a);
            }

            addressConversion.put(a, totalNodes);
            return totalNodes++;
        }

        List<Long> outputAddressesToLongs(Transaction t) {
            List<Long> receivers = new ArrayList<>();

            for (TransactionOutput to: t.getOutputs()) {
                try {
                    Address receiver = to.getScriptPubKey().getToAddress(this.np, true);
                    Long receiverLong = addressToLong(receiver);
                    receivers.add(receiverLong);
                } catch (ScriptException e) {
                    receivers.add(null); // Don't mess up the indexing
                    System.out.println(e.getMessage() + " at " + t.getTxId());
                }
            }

            return receivers;
        }

        @Override
        public boolean hasNext() {
            return bfl.hasNext() || !transactionArcs.isEmpty();
        }

        @Override
        public long[] next() {
            while (true) {
                if (!transactionArcs.isEmpty())
                    return transactionArcs.pop();

                Block block = bfl.next();

                if (!block.hasTransactions())
                    continue;

                for (Transaction t: block.getTransactions()) {
                    if (t.isCoinBase())
                        continue;

                    // Save the incomplete mappings

                    List<Long> receivers = outputAddressesToLongs(t);

                    for (TransactionInput ti: t.getInputs()) {
                        TransactionOutPoint top = ti.getOutpoint();

                        incomplete.putAll(top, receivers);
                        topMapping.put(top.getHash(), top);
                    }

                    // Try completing the mappings

                    List<Long> senders = receivers;
                    Sha256Hash txId = t.getTxId();

                    if (!topMapping.containsKey(txId))
                        continue;

                    for (TransactionOutPoint top: topMapping.remove(txId)) {
                        int index = (int) top.getIndex();
                        List<Long> dedupReceivers = incomplete
                                .remove(top)
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
            }
        }
    }
}
