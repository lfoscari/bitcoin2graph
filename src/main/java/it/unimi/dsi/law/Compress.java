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
    private static final ProgressLogger pl = new ProgressLogger();

    public static void main(String[] args) throws IOException, JSAPException {
        final SimpleJSAP jsap = new SimpleJSAP(ScatteredLabelledArcsASCIIGraph.class.getName(),
                "Compress a given graph using LLP",
                new Parameter[]{
                        new FlaggedOption("tempDir", JSAP.STRING_PARSER, resources.toString(), JSAP.NOT_REQUIRED, 't', "temp-dir", "A directory for all temporary batch files."),
                        new FlaggedOption("batchSize", JSAP.INTEGER_PARSER, "10000000", JSAP.NOT_REQUIRED, 'b', "batch-size", "A directory for all temporary batch files."),
                        new FlaggedOption("oldBasename", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, 'i', "basename", "The basename of the input graph."),
                        new FlaggedOption("newBasename", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, 'o', "new-basename", "The basename of the output graph."),
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

        logger.warn("Loading graph");
        ImmutableGraph graph = ImmutableGraph.loadMapped(oldBasename, pl);

        logger.warn("Symmetrizing");
        graph = Transform.symmetrizeOffline(graph, batchSize, tempDir, pl);

        logger.warn("Removing loops");
        graph =  Transform.filterArcs(graph, NO_LOOPS, pl);

        logger.warn("Temporarily storing graph");
        BVGraph.store(graph, newBasename, pl);
        graph = BVGraph.load(newBasename);

        File clustersFile = newBasenameDir.toPath().resolve("clusters").toFile();

        logger.warn("Computing permutation");
        LayeredLabelPropagation llp = new LayeredLabelPropagation(graph, SEED);
        int[] permutation = llp.computePermutation(clustersFile.toString());

        logger.warn("Applying permutation");
        graph = Transform.mapOffline(graph, permutation, batchSize, tempDir, pl);

        logger.warn("Storing graph");
        BVGraph.store(graph, newBasename, pl);
    }
}
