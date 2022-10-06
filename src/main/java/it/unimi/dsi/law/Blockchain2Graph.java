package it.unimi.dsi.law;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
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
import org.bitcoinj.utils.BlockFileLoader;
import org.bitcoinj.utils.BriefLogFormatter;

public class Blockchain2Graph  {
    Map<TransactionOutPoint, Address[]> incomplete = new HashMap<>();
    MultiValuedMap<Sha256Hash, TransactionOutPoint> topMapping = new ArrayListValuedHashMap<>();
    MultiValuedMap<Address, Address> edges = new ArrayListValuedHashMap<>();
    
    final NetworkParameters np;
    // final String coinbaseAddress = ???;

    public Blockchain2Graph() {
        BriefLogFormatter.init();
        this.np = new MainNetParams();
        new Context(np);
    }

    Address[] outputsToAddresses(Transaction t) {
        List<Address> receivers = new ArrayList<>();
                    
        for (TransactionOutput to: t.getOutputs()) {
            try {
                receivers.add(to.getScriptPubKey().getToAddress(this.np, true));
            } catch (Exception e) {
                System.out.println(e.getMessage() + " at " + t.getTxId());
            }
        }

        return receivers.toArray(Address[]::new);
    }

    void serializeEdgeMap(String destination) {
        try {
            FileOutputStream myFileOutStream = new FileOutputStream(destination);
            ObjectOutputStream myObjectOutStream = new ObjectOutputStream(myFileOutStream);
  
            myObjectOutStream.writeObject(this.edges);
  
            myObjectOutStream.close();
            myFileOutStream.close();
        }
        catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void buildGraph(String blockfile) {
        List<File> blockchainFiles =  new ArrayList<File>();
        blockchainFiles.add(new File(blockfile));
        BlockFileLoader bfl = new BlockFileLoader(np, blockchainFiles);

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

                for (TransactionInput ti: t.getInputs()) {
                    TransactionOutPoint top = ti.getOutpoint();
                    Address[] receivers = outputsToAddresses(t);

                    incomplete.put(top, receivers);
                    topMapping.put(t.getTxId(),top);
                }
            }
        }

        bfl = new BlockFileLoader(np, blockchainFiles);

        for (Block block: bfl) {
            for (Transaction t: block.getTransactions()) {
                Sha256Hash txId = t.getTxId();
                Address[] senders = outputsToAddresses(t);
                
                if (!topMapping.containsKey(txId)) {
                    continue;
                }

                for (TransactionOutPoint top: topMapping.get(txId)) {
                    int index = (int) top.getIndex();

                    for (Address receiver: incomplete.get(top)) {
                        Address sender = senders[index];
                        edges.put(sender, receiver);
                    }
                }
            }
        }

        System.out.println("Done with " + edges.size() + " edges");
    }

    public static void main(String[] args) {
        Blockchain2Graph a = new Blockchain2Graph();

        a.buildGraph("src/main/resources/blk00000.dat");
        a.serializeEdgeMap("src/main/resources/edgelist.txt");
    }
}
