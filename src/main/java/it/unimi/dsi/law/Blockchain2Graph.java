package it.unimi.dsi.law;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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

/* Avoid looping two times over the blocks,
 * because every time they must be loaded from disk.
 */

public class Blockchain2Graph  {
    Map<TransactionOutPoint, Long[]> incomplete = new HashMap<>();
    MultiValuedMap<Sha256Hash, TransactionOutPoint> topMapping = new ArrayListValuedHashMap<>();
    
    MultiValuedMap<Long, Long> edges = new ArrayListValuedHashMap<>();

    HashMap<Address, Long> addressConversion = new HashMap<>();
    public static long totalNodes = 0;

    final NetworkParameters np;
    final static String defaultLocation = "src/main/resources/";

    public Blockchain2Graph() {
        BriefLogFormatter.init();
        this.np = new MainNetParams();
        new Context(this.np);
    }

    Long addressToLong(Address a) {
        /**
         * Map an address to a long without collisions.
         * If a new address is presented generate a new long not seen before.
         * If an old address is presented return the old long association.
         */

        if (addressConversion.containsKey(a)) {
            return addressConversion.get(a);
        }

        addressConversion.put(a, totalNodes);
        return totalNodes++;
    }

    Long[] outputAddressesToLongs(Transaction t) {
        /**
         * Extract the output addresses from a Transaction and map them to longs.
         */

        List<Long> receivers = new ArrayList<>();

        for (TransactionOutput to: t.getOutputs()) {
            try {
                Address receiver = to.getScriptPubKey().getToAddress(this.np, true);
                Long receiverInt = addressToLong(receiver);

                // this.uniqueAddresses.add(receiverInt);
                receivers.add(receiverInt);
            } catch (ScriptException e) {
                receivers.add(null); // Don't mess up the indexing
                // TODO: find out what these addresses actually are
                System.out.println(e.getMessage() + " at " + t.getTxId());
            }
        }

        return receivers.toArray(Long[]::new);
    }

    void multiValuedMap2ASCII(String destination, long totalNodes) throws IOException {
		FileOutputStream os = new FileOutputStream(destination);
        StringBuffer sb = new StringBuffer();

        sb.append(totalNodes + "\n");

        for (Long node = 0L; node < totalNodes; node++) {
            // sb.append(node + " ");

            edges
                .get(node)
                .stream()
                .filter(s -> s != null)
                .sorted()
                .distinct()
                .forEach(s -> sb.append(s + " "));

            if (edges.containsKey(node)) {
                sb.deleteCharAt(sb.length() - 1);
            }

            sb.append("\n");
        }

        os.write(sb.toString().getBytes());
		os.close();

		System.out.println("ASCIIGraph saved in " + destination);
	}

    public void buildGraph(String blockfile) {
        /**
         * Read the blockfile and convert it to a graph in the form of a MultiValuedMap.
         */

        List<File> blockchainFiles = new ArrayList<File>();
        blockchainFiles.add(new File(blockfile));
        BlockFileLoader bfl = new BlockFileLoader(this.np, blockchainFiles);
 
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
                
                Long[] receivers = outputAddressesToLongs(t);

                for (TransactionInput ti: t.getInputs()) {
                    TransactionOutPoint top = ti.getOutpoint();

                    incomplete.put(top, receivers);
                    topMapping.put(top.getHash(), top);
                }
            }
        }

        bfl = new BlockFileLoader(this.np, blockchainFiles);

        for (Block block: bfl) {
            for (Transaction t: block.getTransactions()) {
                Sha256Hash txId = t.getTxId();
                Long[] senders = outputAddressesToLongs(t);
                
                if (!topMapping.containsKey(txId)) {
                    continue;
                }

                for (TransactionOutPoint top: topMapping.get(txId)) {
                    int index = (int) top.getIndex();

                    for (Long receiver: incomplete.get(top)) {
                        Long sender = senders[index];
                        edges.put(sender, receiver);
                    }
                }
            }
        }
    }

    public static void main(String[] args) throws IOException {
        Blockchain2Graph a = new Blockchain2Graph();
        a.buildGraph(defaultLocation + "blk00000.dat");
        a.multiValuedMap2ASCII(defaultLocation + "ascii.graph-txt", totalNodes);
    }
}
