package it.unimi.dsi.law;

import it.unimi.dsi.logging.ProgressLogger;
import it.unimi.dsi.webgraph.BVGraph;
import it.unimi.dsi.webgraph.ScatteredArcsASCIIGraph;
import org.bitcoinj.core.Context;
import org.bitcoinj.core.NetworkParameters;
import org.bitcoinj.params.MainNetParams;
import org.rocksdb.RocksDBException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.ExecutionException;

public class Blockchain2ScatteredArcsASCIIGraph {
    public static void main(String[] args) throws RocksDBException, IOException, ExecutionException, InterruptedException {
        NetworkParameters np = new MainNetParams();
        Context c = new Context(np);
        Context.propagate(c);

        (new File(Parameters.resources + "ScatteredArcsASCIIGraph/")).mkdir();

        Logger logger = LoggerFactory.getLogger(Blockchain2ScatteredArcsASCIIGraph.class);
        ProgressLogger progress = new ProgressLogger(logger, Parameters.logInterval, Parameters.logTimeUnit, "blocks");
        progress.displayFreeMemory = true;
        progress.displayLocalSpeed = true;

        File blocksDirectory = new File(Parameters.resources + "blocks");
        File[] blockFilesArray = blocksDirectory.listFiles((f, p) -> p.startsWith("blk"));

        if (blockFilesArray == null)
            throw new RuntimeException("No block files found!");

        List<File> blockFiles = List.of(blockFilesArray);

        AddressConversion addressConversion = new AddressConversion(np, progress);
        addressConversion.addAddresses(blockFiles);
        addressConversion.close();

        addressConversion = new AddressConversion(np, progress, true);
        CustomBlockchainIterator it = new CustomBlockchainIterator(blockFiles, addressConversion, np, progress);
        it.populateMappings();
        it.completeMappings();

        Path tempDirectory = Files.createTempDirectory(Path.of(Parameters.resources), "scatteredgraph-");
        ScatteredArcsASCIIGraph graph = new ScatteredArcsASCIIGraph(it.iterator(), false, false, 10000, tempDirectory.toFile(), progress);
        BVGraph.store(graph, Parameters.resources + "ScatteredArcsASCIIGraph/" + Parameters.basename, progress);

        progress.stop("Results saved in " + Parameters.resources + "ScatteredArcsASCIIGraph/" + Parameters.basename);
        progress.done();
    }
}
