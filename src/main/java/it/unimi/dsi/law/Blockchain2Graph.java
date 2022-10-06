package it.unimi.dsi.law;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
    Map<Sha256Hash, ArrayList<TransactionOutPoint>> topMapping = new HashMap<>();
    Map<Address, ArrayList<Address>> edges = new HashMap<>();
    NetworkParameters np;

    // Usa una Multimap

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

    void serializeEdgeMap() {
        try {
            FileOutputStream myFileOutStream = new FileOutputStream("./edgelist.txt");
            ObjectOutputStream myObjectOutStream = new ObjectOutputStream(myFileOutStream);
  
            myObjectOutStream.writeObject(this.edges);
  
            myObjectOutStream.close();
            myFileOutStream.close();
        }
        catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void buildGraph() {
        this.np = new MainNetParams();
        new Context(np);

        List<File> blockchainFiles =  new ArrayList<File>();
        blockchainFiles.add(new File("src/main/resources/blk00000.dat"));
        BlockFileLoader bfl = new BlockFileLoader(np, blockchainFiles);

        for (Block block: bfl) {

            // if (block.getTransactions().stream().allMatch((Transaction t) -> t.isCoinBase())) {
            //     // Skip these only because they are boring.
            //     continue;
            // }

            // System.out.println("============== " + block.getHashAsString() + " ==============");

            for(Transaction t: block.getTransactions()) {
                // Sha256Hash txId = t.getTxId();

                // if (t.isCoinBase()) {
                //     System.out.println("*** transaction (coinbase) " + txId + " ***");
                // } else {
                //     System.out.println("*** transaction " + txId +  " - "
                //         + t.getOutputSum().toBtc().stripTrailingZeros().toPlainString() + " BTC ***");
                // }

                // System.out.println("Inputs: ");
                // for (TransactionInput ti: t.getInputs()) {
                //     TransactionOutPoint top = ti.getOutpoint();

                //     if (incomplete.containsKey(top)) {
                //         Address source = incomplete.get(top);
                //         edges.put(, null);
                //     }

                //     if (!t.isCoinBase()) {
                //         System.out.println("\t" + "txid: " + top.getHash() + ", index: " + top.getIndex());
                //     }
                // }

                // System.out.println("Outputs: ");
                // for (TransactionOutput to: t.getOutputs()) {
                //     Address receiver = to.getScriptPubKey().getToAddress(np, true);
                //     System.out.println("\t" + receiver);
                // }

                for (TransactionInput ti: t.getInputs()) {
                    TransactionOutPoint top = ti.getOutpoint();
                    Address[] receivers = outputsToAddresses(t);

                    incomplete.put(top, receivers);

                    if (topMapping.containsKey(t.getTxId())) {
                        topMapping.get(t.getTxId()).add(top);
                    } else {
                        topMapping.put(t.getTxId(), new ArrayList<>(List.of(top)));
                    }
                }

                // System.out.println();
                
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
                    Address[] receivers = incomplete.get(top);
                    int index = (int) top.getIndex();

                    if (index == -1) { // Coinbase
                        continue;
                    }

                    for (Address receiver: receivers) {
                        Address source = senders[index];

                        if (edges.containsKey(source)) {
                            edges.get(source).add(receiver);
                        } else {
                            edges.put(source, new ArrayList<>(List.of(receiver)));
                        }
                    }
                }
            }
        }

        System.out.println("Done with " + edges.size() + " edges");
    }

    public static void main(String[] args) {
        Blockchain2Graph a = new Blockchain2Graph();

        BriefLogFormatter.init();
        a.buildGraph();
        a.serializeEdgeMap();
    }
}
