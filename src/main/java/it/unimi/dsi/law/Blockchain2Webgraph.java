package it.unimi.dsi.law;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.commons.collections4.MultiValuedMap;
import org.apache.commons.collections4.multimap.ArrayListValuedHashMap;
import org.bitcoinj.core.Address;
import org.bitcoinj.core.Block;
import org.bitcoinj.core.Context;
import org.bitcoinj.core.NetworkParameters;
import org.bitcoinj.core.Sha256Hash;
import org.bitcoinj.core.Transaction;
import org.bitcoinj.core.TransactionInput;
import org.bitcoinj.core.TransactionOutPoint;
import org.bitcoinj.core.TransactionOutput;
import org.bitcoinj.params.MainNetParams;
import org.bitcoinj.script.ScriptException;
import org.bitcoinj.utils.BlockFileLoader;
import org.bitcoinj.utils.BriefLogFormatter;

import it.unimi.dsi.webgraph.ArrayListMutableGraph;
import it.unimi.dsi.webgraph.BVGraph;

public class Blockchain2Webgraph  {
    public ArrayListMutableGraph graph;

    public HashMap<Address, Integer> addressConversion = new HashMap<>();
    public static int totalNodes = 0;

    final NetworkParameters np;
    final static String defaultLocation = "src/main/resources/";

    public Blockchain2Webgraph() {
        BriefLogFormatter.init();
        this.np = new MainNetParams();
        new Context(this.np);
    }

    /**
     * Map an address to a Integer without collisions.
     * If a new address is presented generate a new Integer not seen before.
     * If an old address is presented return the old Integer association.
     */
    Integer addressToInteger(Address a) {
        if (addressConversion.containsKey(a)) {
            return addressConversion.get(a);
        }

        addressConversion.put(a, totalNodes);
        return totalNodes++;
    }

    /**
     * Read the blockfile and convert it to a graph in the form of a MultiValuedMap.
     */
    List<Integer> outputAddressesToIntegers(Transaction t) {
        List<Integer> receivers = new ArrayList<>();

        for (TransactionOutput to: t.getOutputs()) {
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

        return receivers;
    }

    /**
     * If needed increase the amount of nodes in the graph,
     * by calculating the difference between the max provided node and the current amount.
     */
    public void increaseNodes(Integer... sortedNodes) {
        Integer max = sortedNodes[sortedNodes.length - 1];
        if (max >= graph.numNodes()) {
            graph.addNodes(max - graph.numNodes() + 1);
        }
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

        for (Block block: bfl) {
            for(Transaction t: block.getTransactions()) {
                if (t.isCoinBase()) {
                    // Address[] outputs = outputsToAddresses(t);
                    // Address sender = Address.fromString(np, coinbaseAddress);
                    // edges.putAll(sender, List.of(outputs));
                    
                    // Ignore these for now, but we might want to add a special coinbase node
                    // and use it as a sender for all coinbase transactions.
                    continue;
                }
                
                List<Integer> receivers = outputAddressesToIntegers(t);

                for (TransactionInput ti: t.getInputs()) {
                    TransactionOutPoint top = ti.getOutpoint();

                    incomplete.putAll(top, receivers);
                    topMapping.put(top.getHash(), top);
                }
            }
        }

        graph = new ArrayListMutableGraph(totalNodes);
        bfl = new BlockFileLoader(this.np, blockchainFiles);

        for (Block block: bfl) {
            for (Transaction t: block.getTransactions()) {
                Sha256Hash txId = t.getTxId();
                List<Integer> senders = outputAddressesToIntegers(t);

                if (!topMapping.containsKey(txId)) {
                    continue;
                }

                for (TransactionOutPoint top: topMapping.get(txId)) {
                    int index = (int) top.getIndex();
                    List<Integer> dedupReceivers = incomplete
                        .get(top)
                        .stream()
                        .filter(n -> n != null && n != index)
                        .sorted()
                        .distinct()
                        .collect(Collectors.toList());

                    increaseNodes(dedupReceivers.toArray(Integer[]::new));

                    for (Integer receiver: dedupReceivers) {
                        Integer sender = senders.get(index);

                        increaseNodes(sender);

                        try {
                            graph.addArc(sender, receiver);
                        } catch(IllegalArgumentException e) {
                            if(!e.getMessage().equals("Node " + receiver + " is already a successor of node " + sender)) {
                                throw new IllegalArgumentException(e.getMessage());
                            }
                        }
                    }
                }
            }
        }
    }

    public static void main(String[] args) throws IOException {
        Blockchain2Webgraph a = new Blockchain2Webgraph();
        a.buildGraph(defaultLocation + "blk00000.dat");
        BVGraph.store(a.graph.immutableView(), defaultLocation + "webgraph/bitcoin");
    }
}
