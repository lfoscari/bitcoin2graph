package it.unimi.dsi.law;

import com.martiansoftware.jsap.*;
import it.unimi.dsi.law.graph.LayeredLabelPropagation;
import it.unimi.dsi.logging.ProgressLogger;
import it.unimi.dsi.webgraph.BVGraph;
import it.unimi.dsi.webgraph.ImmutableGraph;
import it.unimi.dsi.webgraph.Transform;
import it.unimi.dsi.webgraph.labelling.ScatteredLabelledArcsASCIIGraph;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;

import static it.unimi.dsi.law.Parameters.*;
import static it.unimi.dsi.webgraph.Transform.NO_LOOPS;

public class Compress {
    private static final int SEED = 33;
    private static final Logger logger = LoggerFactory.getLogger(Compress.class);
    private static final ProgressLogger pl = new ProgressLogger(logger);

    public static void main(String[] args) throws IOException, JSAPException {
        final SimpleJSAP jsap = new SimpleJSAP(ScatteredLabelledArcsASCIIGraph.class.getName(),
                "Compress a given graph using LLP",
                new Parameter[]{
                        new FlaggedOption("tempDir", JSAP.STRING_PARSER, resources.toString(), JSAP.NOT_REQUIRED, 't', "temp-dir", "A directory for all temporary batch files."),
                        new FlaggedOption("batchSize", JSAP.INTEGER_PARSER, "10000000", JSAP.NOT_REQUIRED, 'b', "batch-size", "A directory for all temporary batch files."),
                        new FlaggedOption("clustersDir", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.NOT_REQUIRED, 'c', "clusters-dir", "A directory for clusters."),
                        new FlaggedOption("oldBasename", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, 'i', "basename", "The basename of the input graph."),
                        new FlaggedOption("newBasename", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, 'o', "new-basename", "The basename of the output graph."),
                        new Switch("simplify", 's', "simplify", "Perform the simplification and save the resulting graph, this step is necessary for compression."),
                }
        );

        final JSAPResult jsapResult = jsap.parse(args);
        if (jsap.messagePrinted()) System.exit(1);

        File tempDir = new File(jsapResult.getString("tempDir"));
        if (!tempDir.exists()) {
            logger.warn(tempDir + " does not exist, creating it");
            tempDir.mkdir();
        }

        int batchSize = jsapResult.getInt("batchSize");

        String oldBasename = jsapResult.getString("oldBasename");
        String newBasename = jsapResult.getString("newBasename");

        File newBasenameDir = new File(newBasename).toPath().getParent().toFile();
        if (!newBasenameDir.exists()) {
            logger.warn(newBasenameDir + " does not exist, creating it");
            newBasenameDir.mkdir();
        }

        logger.info("Loading graph");
        ImmutableGraph graph = ImmutableGraph.loadMapped(oldBasename, pl);

        if (jsapResult.contains("simplify")) {
            logger.info("Symmetrizing");
            graph = Transform.symmetrizeOffline(graph, batchSize, tempDir, pl);

            logger.info("Removing loops");
            graph =  Transform.filterArcs(graph, NO_LOOPS, pl);
        } else {
            if (!jsapResult.contains("clustersDir")) throw new JSAPException("Provide a clusters directory when computing permutations");

            File clustersDir = new File(jsapResult.getString("clustersDir"));
            if (!clustersDir.exists()) {
                logger.warn(clustersDir + " does not exist, creating it");
                clustersDir.mkdir();
            }

            logger.info("Computing permutation");
            LayeredLabelPropagation llp = new LayeredLabelPropagation(graph, SEED);
            int[] permutation = llp.computePermutation(clusterFile.toString());

            logger.info("Applying permutation");
            graph = Transform.mapOffline(graph, permutation, batchSize, tempDir, pl);
        }

        logger.info("Storing graph");
        BVGraph.store(graph, newBasename, pl);
    }
}
