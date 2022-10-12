package it.unimi.dsi.law;

import it.unimi.dsi.webgraph.ArrayListMutableGraph;
import it.unimi.dsi.webgraph.BVGraph;
import org.apache.commons.collections4.MultiValuedMap;
import org.apache.commons.collections4.multimap.ArrayListValuedHashMap;
import org.bitcoinj.core.*;
import org.bitcoinj.params.MainNetParams;
import org.bitcoinj.script.ScriptException;
import org.bitcoinj.utils.BlockFileLoader;
import org.bitcoinj.utils.BriefLogFormatter;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;

public class Blockchain2ArrayListMutableGraph {
    public static int totalNodes = 0;
    final NetworkParameters np;
    public ArrayListMutableGraph graph;
    public HashMap<Address, Integer> addressConversion = new HashMap<>();

    public Blockchain2ArrayListMutableGraph() {
        BriefLogFormatter.init();
        this.np = new MainNetParams();
        new Context(this.np);
    }

    public static void main(String[] args) throws IOException {
        Blockchain2ArrayListMutableGraph a = new Blockchain2ArrayListMutableGraph();
        a.buildGraph(Parameters.resources + Parameters.blockfile);
        BVGraph.store(a.graph.immutableView(), Parameters.resources + "ArrayListMutableGraph/" + Parameters.basename);
    }

    /**
     * Map an address to a Integer without collisions.
     * If a new address is presented generate a new Integer not seen before.
     * If an old address is presented return the old Integer association.
     */
    Integer addressToInteger(Address a) {
        if (addressConversion.containsKey(a))
            return addressConversion.get(a);

        addressConversion.put(a, totalNodes);
        return totalNodes++;
    }

    /**
     * Read the blockfile and convert it to a graph in the form of a MultiValuedMap.
     */
    List<Integer> outputAddressesToIntegers(Transaction t) {
        List<Integer> receivers = new ArrayList<>();

        for (TransactionOutput to : t.getOutputs()) {
            try {
                Address receiver = to.getScriptPubKey().getToAddress(this.np, true);
                Integer receiverInteger = addressToInteger(receiver);
                receivers.add(receiverInteger);
            } catch (ScriptException e) {
                receivers.add(null); // Don't mess up the indexing
                // TODO: find out what these addresses actually are
                System.out.println(e.getMessage() + " at " + t.getTxId());
            }
        }

        increaseNodes();
        return receivers;
    }

    /**
     * If needed increase the amount of nodes in the graph,
     * by calculating the difference between the max provided node and the current amount.
     */
    public void increaseNodes() {
        if (graph == null)
            return;

        if (totalNodes >= graph.numNodes())
            graph.addNodes(totalNodes - graph.numNodes() + 1);
    }

    /**
     * Read the blockfile and convert it to a graph in the form of a MultiValuedMap.
     */
    public void buildGraph(String blockfile) {
        List<File> blockchainFiles = new ArrayList<File>();
        blockchainFiles.add(new File(blockfile));
        BlockFileLoader bfl = new BlockFileLoader(this.np, blockchainFiles);

        MultiValuedMap<TransactionOutPoint, Integer> incomplete = new ArrayListValuedHashMap<>();
        MultiValuedMap<Sha256Hash, TransactionOutPoint> topMapping = new ArrayListValuedHashMap<>();

        for (Block block : bfl) {
            for (Transaction t : block.getTransactions()) {
                if (t.isCoinBase())
                    // Address[] outputs = outputsToAddresses(t);
                    // Address sender = Address.fromString(np, coinbaseAddress);
                    // edges.putAll(sender, List.of(outputs));

                    // Ignore these for now, but we might want to add a special coinbase node
                    // and use it as a sender for all coinbase transactions.
                    continue;

                List<Integer> receivers = outputAddressesToIntegers(t);

                for (TransactionInput ti : t.getInputs()) {
                    TransactionOutPoint top = ti.getOutpoint();

                    incomplete.putAll(top, receivers);
                    topMapping.put(top.getHash(), top);
                }
            }
        }

        graph = new ArrayListMutableGraph(totalNodes);
        bfl = new BlockFileLoader(this.np, blockchainFiles);

        for (Block block : bfl) {
            for (Transaction t : block.getTransactions()) {
                Sha256Hash txId = t.getTxId();
                List<Integer> senders = outputAddressesToIntegers(t);

                if (!topMapping.containsKey(txId))
                    continue;

                for (TransactionOutPoint top : topMapping.remove(txId)) {
                    int index = (int) top.getIndex();
                    List<Integer> dedupReceivers = incomplete
                            .remove(top)
                            .stream()
                            .filter(n -> n != null && n != index)
                            .sorted()
                            .distinct()
                            .collect(Collectors.toList());

                    for (Integer receiver : dedupReceivers) {
                        Integer sender = senders.get(index);

                        try {
                            graph.addArc(sender, receiver);
                        } catch (IllegalArgumentException e) { // This wastes a LOT of time
                            if (!e.getMessage().equals("Node " + receiver + " is already a successor of node " + sender))
                                throw new IllegalArgumentException(e.getMessage());
                        }
                    }
                }
            }
        }
    }
}
