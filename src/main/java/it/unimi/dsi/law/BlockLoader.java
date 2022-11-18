package it.unimi.dsi.law;

import it.unimi.dsi.fastutil.objects.ObjectArrayFIFOQueue;
import it.unimi.dsi.logging.ProgressLogger;
import org.bitcoinj.core.NetworkParameters;
import org.bitcoinj.core.Utils;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;

import static org.bitcoinj.core.Block.BLOCK_HEIGHT_GENESIS;

public class BlockLoader implements Runnable, Iterable<byte[]>, Iterator<byte[]> {
    public final List<File> blockFiles;
    private final Iterator<File> blockFilesIt;
    private final LinkedBlockingQueue<List<byte[]>> blockQueue;
    private boolean genesis = true;
    private final ProgressLogger progress;
    private final NetworkParameters np;

    private final ObjectArrayFIFOQueue<byte[]> buffer;

    public BlockLoader (List<File> blockFiles, LinkedBlockingQueue<List<byte[]>> blockQueue, ProgressLogger progress, NetworkParameters np) {
        this.blockFiles = blockFiles;
        this.blockFilesIt = blockFiles.iterator();

        // to remove
        this.buffer = new ObjectArrayFIFOQueue<>();

        this.blockQueue = blockQueue;
        this.progress = progress;
        this.np = np;
    }

    public List<byte[]> loadNextBlocks () throws IOException {
        File blockFile = this.blockFilesIt.next();

        byte[] blocks = Files.readAllBytes(blockFile.toPath());
        ByteArrayInputStream bis = new ByteArrayInputStream(blocks);
        List<byte[]> blockList = new ArrayList<>();

        if (this.genesis) {
            bis.readNBytes(BLOCK_HEIGHT_GENESIS);
            this.genesis = false;
        }

        while (true) {
            int nextChar = bis.read();
            while (nextChar != -1) {
                if (nextChar != ((this.np.getPacketMagic() >>> 24) & 0xff)) {
                    nextChar = bis.read();
                    continue;
                }
                nextChar = bis.read();
                if (nextChar != ((this.np.getPacketMagic() >>> 16) & 0xff)) {
                    continue;
                }
                nextChar = bis.read();
                if (nextChar != ((this.np.getPacketMagic() >>> 8) & 0xff)) {
                    continue;
                }
                nextChar = bis.read();
                if (nextChar == (this.np.getPacketMagic() & 0xff)) {
                    break;
                }
            }

            byte[] bytes = new byte[4];
            if (bis.read(bytes, 0, 4) == -1) {
                break;
            }

            long size = Utils.readUint32BE(Utils.reverseBytes(bytes), 0);

            bytes = new byte[(int) size];
            if (bis.read(bytes, 0, (int) size) == -1) {
                break;
            }

            blockList.add(bytes);
        }

        // this.progress.logger.info("Loaded blockfile " + blockFile.getName());

        return blockList;
    }

    // TO REMOVE
    public byte[] next() {
        if (this.buffer.isEmpty()) {
            try {
                this.loadNextBlocks().forEach(this.buffer::enqueue);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        return this.buffer.dequeue();
    }

    public boolean hasNext() {
        return this.blockFilesIt.hasNext(); //  || this.blockQueue.size() > 0;
    }

    @Override
    public void run () {
        while (this.blockFilesIt.hasNext()) {
            try {
                if (this.blockQueue.remainingCapacity() == 0) {
                    continue;
                }

                List<byte[]> blocks = this.loadNextBlocks();

                if (blocks.size() > 0) {
                    this.blockQueue.put(blocks);
                }
            } catch (IOException | InterruptedException e) {
                throw new RuntimeException(e);
            }
        }

        this.progress.logger.info("All blockfiles loaded!");
    }

    @Override
    public Iterator<byte[]> iterator() {
        return this;
    }
}