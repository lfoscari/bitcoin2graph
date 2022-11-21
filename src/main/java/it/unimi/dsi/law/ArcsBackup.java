package it.unimi.dsi.law;

import it.unimi.dsi.fastutil.Pair;
import it.unimi.dsi.fastutil.io.FastBufferedOutputStream;
import it.unimi.dsi.law.utils.BitcoinUtils;
import org.bitcoinj.core.NetworkParameters;

import java.io.*;
import java.util.concurrent.LinkedBlockingQueue;

public class ArcsBackup implements Runnable {
    private final LinkedBlockingQueue<Pair<byte[], byte[]>> arcs;
    private final NetworkParameters np;
    public volatile boolean stop;
    private final FastBufferedOutputStream backup;

    public ArcsBackup(LinkedBlockingQueue<Pair<byte[], byte[]>> arcs, NetworkParameters np) throws FileNotFoundException {
        this.arcs = arcs;
        this.np = np;

        this.stop = false;

        File backupFile = new File(Parameters.resources + "arcs");
        OutputStream os = new FileOutputStream(backupFile);
        this.backup = new FastBufferedOutputStream(os);
    }

    @Override
    public void run() {
        while (!this.stop) {
            try {
                Pair<byte[], byte[]> arc = this.arcs.take();
                byte[] sender = arc.left(), receiver = arc.right();

                this.backup.write((BitcoinUtils.addressToString(sender, this.np) +
                        " -> " + BitcoinUtils.addressToString(receiver, this.np)).getBytes());
            } catch (IOException | InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
