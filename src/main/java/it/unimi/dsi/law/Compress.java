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

import static it.unimi.dsi.webgraph.Transform.NO_LOOPS;

public class Compress {
    private static final int SEED = 33;
    private static final Logger logger = LoggerFactory.getLogger(Compress.class);
    private static final ProgressLogger pl = new ProgressLogger();

    public static void main(String[] args) throws IOException, JSAPException {
        final SimpleJSAP jsap = new SimpleJSAP(ScatteredLabelledArcsASCIIGraph.class.getName(),
                "Compress a given graph using LLP",
                new Parameter[]{
                        new UnflaggedOption("oldBasename", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, false, "The basename of the input graph."),
                        new UnflaggedOption("newBasename", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, false, "The basename of the output graph."),
                }
        );

        final JSAPResult jsapResult = jsap.parse(args);
        if (jsap.messagePrinted()) System.exit(1);

        String oldBasename = jsapResult.getString("oldBasename");
        String newBasename = jsapResult.getString("newBasename");

        File newBasenameDir = new File(newBasename).toPath().getParent().toFile();
        if (!newBasenameDir.exists()) {
            logger.warn(newBasenameDir + " does not exist, creating it");
            newBasenameDir.mkdir();
        }

        File clustersFile = newBasenameDir.toPath().resolve("clusters").toFile();

        logger.warn("Loading graph");
        ImmutableGraph graph = ImmutableGraph.load(oldBasename, pl);

        logger.warn("Symmetrizing");
        graph = Transform.symmetrize(graph, pl);

        logger.warn("Removing loops");
        graph =  Transform.filterArcs(graph, NO_LOOPS, pl);

        logger.warn("Temporarily storing graph");
        // This is needed to obtain a fully computed union of the graph and its transpose
        BVGraph.store(graph, newBasename, pl);
        graph = ImmutableGraph.load(newBasename);

        logger.warn("Computing permutation");
        LayeredLabelPropagation llp = new LayeredLabelPropagation(graph, SEED);
        int[] permutation = llp.computePermutation(clustersFile.toString());

        logger.warn("Applying permutation");
        graph = Transform.map(graph, permutation, pl);

        logger.warn("Storing graph");
        BVGraph.store(graph, newBasename, pl);
    }
}
