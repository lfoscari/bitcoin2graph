package it.unimi.dsi.law;

import it.unimi.dsi.fastutil.objects.Object2ObjectOpenHashMap;
import org.bitcoinj.core.*;
import org.bitcoinj.params.MainNetParams;
import org.bitcoinj.script.Script;
import org.bitcoinj.script.ScriptException;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

import static it.unimi.dsi.law.Parameters.MISSING_ADDRESS;
import static it.unimi.dsi.law.Parameters.UNKNOWN_ADDRESS;

public class OnlineParsing {
    private final Object2ObjectOpenHashMap<byte[], byte[][]> map;
    private final Object2ObjectOpenHashMap<byte[], byte[]> arcs;
    private final List<File> blockFiles;
    private final NetworkParameters np;

    public static void main(String[] args) throws IOException {
        OnlineParsing op = new OnlineParsing();
        op.run();
    }

    public OnlineParsing() {
        this.map = new Object2ObjectOpenHashMap<>();
        this.arcs = new Object2ObjectOpenHashMap<>();
        this.blockFiles = getBlockFiles(Parameters.resources + "blocks");

        this.np = new MainNetParams();
        new Context(this.np);
    }
    
    public void run() throws IOException {
        BlockLoader bl = new BlockLoader(this.blockFiles, null, null, this.np);

        while (bl.hasNext()) {
            for (byte[] blockBytes : bl.loadNextBlocks()) {
                Block block = this.np.getDefaultSerializer().makeBlock(blockBytes);

                if (!block.hasTransactions()) {
                    continue;
                }

                for (Transaction transaction : block.getTransactions()) {
                    byte[][] outputs = this.getOutputAddresses(transaction);
                    this.map.put(transaction.getTxId().getBytes(), outputs);

                    if (transaction.isCoinBase()) {
                        continue;
                    }

                    List<TransactionInput> senders = transaction.getInputs();

                    for (TransactionInput ti : senders) {
                        TransactionOutPoint top = ti.getOutpoint();
                        Sha256Hash txId = top.getHash();
                        long index = top.getIndex();

                        byte[][] sendersAddresses = this.map.get(txId.getBytes());

                        if (sendersAddresses == null) {
                            continue;
                        }

                        byte[] senderAddress = sendersAddresses[(int) index];

                        if (senderAddress == null) {
                            continue;
                        }

                        for (byte[] outputAddress : outputs) {
                            this.arcs.put(senderAddress, outputAddress);
                        }
                    }
                }
            }
        }

        this.saveArcs();
        this.saveMap();
    }

    public void saveArcs() {
        File arcs = new File(Parameters.resources + "arcs.txt");
        try (FileOutputStream os = new FileOutputStream(arcs)) {
            this.arcs.forEach((senderBytes, receiverBytes) -> {
                String sender = LegacyAddress.fromPubKeyHash(this.np, senderBytes).toBase58();
                String receiver = LegacyAddress.fromPubKeyHash(this.np, receiverBytes).toBase58();

                try {
                    os.write((sender + " -> " + receiver + "\n").getBytes());
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            });
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void saveMap() {
        File map = new File(Parameters.resources + "map.txt");
        try (FileOutputStream os = new FileOutputStream(map)) {
            this.map.forEach((txIdBytes, addressesBytes) -> {
                String txId = Sha256Hash.wrap(txIdBytes).toString();

                StringBuilder addresses = new StringBuilder();
                for (byte[] address : addressesBytes) {
                    if (address == MISSING_ADDRESS) {
                        addresses.append("MISSING");
                    } else if (address == UNKNOWN_ADDRESS) {
                        addresses.append("UNKNOWN");
                    } else {
                        addresses.append(LegacyAddress.fromPubKeyHash(this.np, address).toBase58());
                    }
                    addresses.append(" ");
                }

                try {
                    os.write((txId + " -> " + addresses + "\n").getBytes());
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            });
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static List<File> getBlockFiles (String blocksDirName) {
        File blocksDir = new File(blocksDirName);
        List<File> list = new ArrayList<>();
        for (int i = 0; true; i++) {
            File file = new File(blocksDir, String.format(Locale.US, "blk%05d.dat", i));
            if (!file.exists()) {
                break;
            }
            list.add(file);
        }
        return list;
    }

    public byte[][] getOutputAddresses (Transaction t) {
        return t.getOutputs().stream().map(this::transactionOutputToAddress).toArray(byte[][]::new);
    }

    public byte[] transactionOutputToAddress (TransactionOutput to) {
        try {
            Script script = to.getScriptPubKey();

            if (script.getScriptType() == null) {
                // No public keys are contained in this script.
                return MISSING_ADDRESS;
            }

            return script.getToAddress(this.np, true).getHash();
        } catch (IllegalArgumentException | ScriptException e) {
            // Non-standard address
            return UNKNOWN_ADDRESS;
        }
    }
}
