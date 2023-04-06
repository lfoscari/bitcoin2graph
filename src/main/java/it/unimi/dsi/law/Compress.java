package it.unimi.dsi.law;

import com.martiansoftware.jsap.*;
import it.unimi.dsi.law.graph.LayeredLabelPropagation;
import it.unimi.dsi.logging.ProgressLogger;
import it.unimi.dsi.webgraph.BVGraph;
import it.unimi.dsi.webgraph.ImmutableGraph;
import it.unimi.dsi.webgraph.Transform;
import it.unimi.dsi.webgraph.labelling.ScatteredLabelledArcsASCIIGraph;

import java.io.File;
import java.io.IOException;

import static it.unimi.dsi.law.Parameters.*;

public class Compress {
    private static final int SEED = 33;
    private static final ProgressLogger pl = new ProgressLogger();

    public static void main(String[] args) throws IOException, JSAPException {
        final SimpleJSAP jsap = new SimpleJSAP(ScatteredLabelledArcsASCIIGraph.class.getName(),
                "Compress a given graph using LLP",
                new Parameter[]{
                        new FlaggedOption("tempDir", JSAP.STRING_PARSER, resources.toString(), JSAP.NOT_REQUIRED, 't', "temp-dir", "A directory for all temporary batch files."),
                        new FlaggedOption("batchSize", JSAP.INTEGER_PARSER, "10_000_000", JSAP.NOT_REQUIRED, 'b', "batch-size", "A directory for all temporary batch files."),
                        new FlaggedOption("clustersDir", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, 'c', "clusters-dir", "A directory for clusters."),
                        new FlaggedOption("oldBasename", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, 'i', "basename", "The basename of the input graph."),
                        new FlaggedOption("newBasename", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, 'o', "new-basename", "The basename of the output graph."),
                }
        );

        final JSAPResult jsapResult = jsap.parse(args);
        if (jsap.messagePrinted()) System.exit(1);

        File tempDir = new File(jsapResult.getString("tempDir"));
        if (!tempDir.exists()) {
            pl.logger.warn(tempDir + " does not exist, creating it");
            tempDir.mkdir();
        }

        int batchSize = jsapResult.getInt("batchSize");

        File clustersDir = new File(jsapResult.getString("clustersDir"));
        if (!clustersDir.exists()) {
            pl.logger.warn(clustersDir + " does not exist, creating it");
            clustersDir.mkdir();
        }

        String oldBasename = jsapResult.getString("oldBasename");
        String newBasename = jsapResult.getString("newBasename");

        if (!new File(newBasename).toPath().getParent().toFile().exists()) {
            throw new JSAPException(newBasename + " is not on an existing path");
        }

        pl.logger.info("Loading graph");
        ImmutableGraph graph = ImmutableGraph.loadMapped(oldBasename, pl);

        pl.logger.info("Symmetrizing");
        graph = Transform.symmetrizeOffline(graph, batchSize, tempDir, pl);

        pl.logger.info("Removing loops");
        graph =  Transform.filterArcs(graph, Transform.NO_LOOPS, pl);

        pl.logger.info("Permuting");
        LayeredLabelPropagation llp = new LayeredLabelPropagation(graph, SEED);
        int[] permutation = llp.computePermutation(clusterFile.toString());

        pl.logger.info("Applying permutation");
        graph = Transform.mapOffline(graph, permutation, batchSize, tempDir, pl);

        pl.logger.info("Storing graph");
        BVGraph.store(graph, newBasename, pl);
    }
}
