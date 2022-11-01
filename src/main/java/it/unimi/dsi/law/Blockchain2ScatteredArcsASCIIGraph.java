package it.unimi.dsi.law;

import it.unimi.dsi.law.utils.ByteConversion;
import org.bitcoinj.utils.BlockFileLoader;

import it.unimi.dsi.fastutil.longs.LongArrayFIFOQueue;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.longs.LongList;
import it.unimi.dsi.law.persistence.*;
import it.unimi.dsi.logging.ProgressLogger;
import it.unimi.dsi.webgraph.BVGraph;
import it.unimi.dsi.webgraph.ScatteredArcsASCIIGraph;
import org.bitcoinj.core.*;
import org.bitcoinj.params.MainNetParams;
import org.bitcoinj.script.Script;
import org.rocksdb.Holder;
import org.rocksdb.RocksDBException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.*;
import java.util.stream.Collectors;

import static org.bitcoinj.script.Script.ScriptType.P2WPKH;
import static org.bitcoinj.utils.BlockFileLoader.getReferenceClientBlockFileList;

public class Blockchain2ScatteredArcsASCIIGraph { // implements Iterable<long[]> {
    public final NetworkParameters np;
    public final List<File> blockFiles;
    private final ProgressLogger progress;

    public Blockchain2ScatteredArcsASCIIGraph(List<File> blockFiles, ProgressLogger progress) {
        this.blockFiles = blockFiles;
        this.np = new MainNetParams();
        this.progress = progress;
    }

    public static void main(String[] args) {
        (new File(Parameters.resources + "ScatteredArcsASCIIGraph/")).mkdir();

        Logger logger = LoggerFactory.getLogger(Blockchain2ScatteredArcsASCIIGraph.class);
        ProgressLogger progress = new ProgressLogger(logger, Parameters.logInterval, Parameters.logTimeUnit, "blocks");
        progress.displayFreeMemory = true;
        progress.displayLocalSpeed = true;

        File blocksDirectory = new File(Parameters.resources);
        File[] blockFiles = blocksDirectory.listFiles((f, p) -> p.startsWith("blk"));

        if (blockFiles == null)
            throw new RuntimeException("No block files found!");

        Blockchain2ScatteredArcsASCIIGraph bt = new Blockchain2ScatteredArcsASCIIGraph(Arrays.asList(blockFiles), progress);
        bt.go();

        /* ScatteredArcsASCIIGraph graph = new ScatteredArcsASCIIGraph(bt.iterator(), false, false, 10000, null, progress);
        BVGraph.store(graph, Parameters.resources + "ScatteredArcsASCIIGraph/" + Parameters.basename, progress); */

        progress.stop("Results saved in " + Parameters.resources + "ScatteredArcsASCIIGraph/" + Parameters.basename);
        progress.done();
    }

    public void go() {
        try {
            new CustomBlockchainIterator(blockFiles, np, progress);
        } catch (RocksDBException | IOException e) {
            throw new RuntimeException(e);
        }
    }

    /* @Override
    public Iterator<long[]> iterator() {
        try {
            return new CustomBlockchainIterator<long[]>(blockFiles, np, progress);
        } catch (RocksDBException | IOException e) {
            throw new RuntimeException(e);
        }
    } */
}
