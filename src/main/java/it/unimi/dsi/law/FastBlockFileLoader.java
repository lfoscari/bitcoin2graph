package it.unimi.dsi.law;

import static com.google.common.base.Preconditions.checkArgument;

import it.unimi.dsi.fastutil.io.FastBufferedInputStream;
import org.bitcoinj.core.Block;
import org.bitcoinj.core.NetworkParameters;
import org.bitcoinj.core.ProtocolException;
import org.bitcoinj.core.Utils;
import org.bitcoinj.utils.AppDataDirectory;

import java.io.*;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.NoSuchElementException;

public class FastBlockFileLoader implements Iterable<Block>, Iterator<Block> {
    /**
     * Gets the list of files which contain blocks from Bitcoin Core.
     */
    public static List<File> getReferenceClientBlockFileList(File blocksDir) {
        checkArgument(blocksDir.isDirectory(), "%s is not a directory", blocksDir);
        List<File> list = new LinkedList<>();
        for (int i = 0; true; i++) {
            File file = new File(blocksDir, String.format(Locale.US, "blk%05d.dat", i));
            if (!file.exists())
                break;
            list.add(file);
        }
        return list;
    }

    public static List<File> getReferenceClientBlockFileList() {
        return getReferenceClientBlockFileList(defaultBlocksDir());
    }

    public static File defaultBlocksDir() {
        File defaultBlocksDir = AppDataDirectory.getPath("Bitcoin").resolve("blocks").toFile();
        if (!defaultBlocksDir.isDirectory())
            throw new RuntimeException("Default blocks directory not found");
        return defaultBlocksDir;
    }

    private Iterator<File> fileIt;
    private File file = null;
    private FastBufferedInputStream currentFileStream = null;
    private Block nextBlock = null;
    private NetworkParameters params;

    public FastBlockFileLoader(NetworkParameters params, List<File> files) {
        fileIt = files.iterator();
        this.params = params;
    }

    @Override
    public boolean hasNext() {
        if (nextBlock == null)
            loadNextBlock();
        return nextBlock != null;
    }

    @Override
    public Block next() throws NoSuchElementException {
        if (!hasNext())
            throw new NoSuchElementException();
        Block next = nextBlock;
        nextBlock = null;
        return next;
    }

    private void loadNextBlock() {
        while (true) {
            try {
                if (!fileIt.hasNext() && (currentFileStream == null || currentFileStream.available() < 1))
                    break;
            } catch (IOException e) {
                currentFileStream = null;
                if (!fileIt.hasNext())
                    break;
            }
            while (true) {
                try {
                    if (currentFileStream != null && currentFileStream.available() > 0)
                        break;
                } catch (IOException e1) {
                    currentFileStream = null;
                }
                if (!fileIt.hasNext()) {
                    nextBlock = null;
                    currentFileStream = null;
                    return;
                }
                file = fileIt.next();
                try {
                    currentFileStream = new FastBufferedInputStream(new FileInputStream(file));
                } catch (FileNotFoundException e) {
                    currentFileStream = null;
                }
            }
            try {
                int nextChar = currentFileStream.read();
                while (nextChar != -1) {
                    if (nextChar != ((params.getPacketMagic() >>> 24) & 0xff)) {
                        nextChar = currentFileStream.read();
                        continue;
                    }
                    nextChar = currentFileStream.read();
                    if (nextChar != ((params.getPacketMagic() >>> 16) & 0xff))
                        continue;
                    nextChar = currentFileStream.read();
                    if (nextChar != ((params.getPacketMagic() >>> 8) & 0xff))
                        continue;
                    nextChar = currentFileStream.read();
                    if (nextChar == (params.getPacketMagic() & 0xff))
                        break;
                }
                byte[] bytes = new byte[4];
                currentFileStream.read(bytes, 0, 4);
                long size = Utils.readUint32BE(Utils.reverseBytes(bytes), 0);
                bytes = new byte[(int) size];
                currentFileStream.read(bytes, 0, (int) size);
                try {
                    nextBlock = params.getDefaultSerializer().makeBlock(bytes);
                } catch (ProtocolException e) {
                    nextBlock = null;
                    continue;
                } catch (Exception e) {
                    throw new RuntimeException("unexpected problem with block in " + file, e);
                }
                break;
            } catch (IOException e) {
                currentFileStream = null;
            }
        }
    }

    @Override
    public void remove() throws UnsupportedOperationException {
        throw new UnsupportedOperationException();
    }

    @Override
    public Iterator<Block> iterator() {
        return this;
    }
}

