package it.unimi.dsi.law;

import com.martiansoftware.jsap.*;
import it.unimi.dsi.fastutil.io.BinIO;
import it.unimi.dsi.law.graph.LayeredLabelPropagation;
import it.unimi.dsi.logging.ProgressLogger;
import it.unimi.dsi.webgraph.BVGraph;
import it.unimi.dsi.webgraph.ImmutableGraph;
import it.unimi.dsi.webgraph.Transform;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;

public class Compress {
    private static final int SEED = 33;
    private static final Logger logger = LoggerFactory.getLogger(Compress.class);
    private static final ProgressLogger pl = new ProgressLogger(logger);

    public static void main(String[] args) throws IOException, JSAPException {
        final SimpleJSAP jsap = new SimpleJSAP(Compress.class.getName(), "Compress a given graph using LLP",
                new Parameter[] {
                        new UnflaggedOption("basename", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, false, "The basename of the graph."),
                        new UnflaggedOption("simplifiedBasename", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, false, "The basename of the simplified graph."),
                }
        );

        final JSAPResult jsapResult = jsap.parse(args);
        if (jsap.messagePrinted()) System.exit(1);

        String basename = jsapResult.getString("basename");
        String simplifiedBasename = jsapResult.getString("simplifiedBasename");

        File newBasenameDir = new File(basename).toPath().getParent().resolve("compressed").toFile();
        if (!newBasenameDir.exists()) {
            logger.warn(newBasenameDir + " does not exist, creating it");
            newBasenameDir.mkdir();
        }

        File clustersFile = newBasenameDir.toPath().resolve("clusters").toFile();
        File permFile = newBasenameDir.toPath().resolve("permutation").toFile();

        logger.info("Loading simplified graph");
        ImmutableGraph graph = BVGraph.load(simplifiedBasename, pl);

        logger.info("Computing permutation");
        LayeredLabelPropagation llp = new LayeredLabelPropagation(graph, SEED);
        int[] permutation = llp.computePermutation(clustersFile.toString());

        logger.info("Storing permutation");
        BinIO.storeInts(permutation, permFile.toString());

        logger.info("Loading original graph");
        graph = ImmutableGraph.load(basename, pl);

        logger.info("Applying permutation");
        graph = Transform.map(graph, permutation, pl);

        logger.info("Storing graph");
        BVGraph.store(graph, newBasenameDir.toPath().resolve(new File(simplifiedBasename).getName()).toString(), pl);
    }
}
