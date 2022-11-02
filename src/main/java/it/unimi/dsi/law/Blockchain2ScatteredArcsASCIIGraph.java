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
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import static org.bitcoinj.script.Script.ScriptType.P2WPKH;
import static org.bitcoinj.utils.BlockFileLoader.getReferenceClientBlockFileList;

public class Blockchain2ScatteredArcsASCIIGraph { // implements Iterable<long[]> {
    public static void main(String[] args) throws RocksDBException, IOException, ExecutionException, InterruptedException {
        NetworkParameters np = new MainNetParams();

        (new File(Parameters.resources + "ScatteredArcsASCIIGraph/")).mkdir();

        Logger logger = LoggerFactory.getLogger(Blockchain2ScatteredArcsASCIIGraph.class);
        ProgressLogger progress = new ProgressLogger(logger, Parameters.logInterval, Parameters.logTimeUnit, "blocks");
        progress.displayFreeMemory = true;
        progress.displayLocalSpeed = true;

        File blocksDirectory = new File(Parameters.resources);
        File[] blockFilesArray = blocksDirectory.listFiles((f, p) -> p.startsWith("blk"));

        if (blockFilesArray == null)
            throw new RuntimeException("No block files found!");

        List<File> blockFiles = List.of(blockFilesArray);

        AddressConversion addressConversion = new AddressConversion(progress);
        addressConversion.addAddresses(blockFiles);
        addressConversion.freeze();

        CustomBlockchainIterator it = new CustomBlockchainIterator(blockFiles, addressConversion, np, progress);
        it.populateMappings();

        /* ScatteredArcsASCIIGraph graph = new ScatteredArcsASCIIGraph(bt.iterator(), false, false, 10000, null, progress);
        BVGraph.store(graph, Parameters.resources + "ScatteredArcsASCIIGraph/" + Parameters.basename, progress); */

        progress.stop("Results saved in " + Parameters.resources + "ScatteredArcsASCIIGraph/" + Parameters.basename);
        progress.done();
    }
}
