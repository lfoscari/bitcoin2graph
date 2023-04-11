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
    private static final ProgressLogger pl = new ProgressLogger(logger);

    public static void main(String[] args) throws IOException, JSAPException {
        final SimpleJSAP jsap = new SimpleJSAP(Compress.class.getName(), "Compress a given graph using LLP",
                new Parameter[] {
                        new FlaggedOption("batchSize", JSAP.INTEGER_PARSER, "10000000", JSAP.NOT_REQUIRED, 'b', "The batch size."),
                        new FlaggedOption("tempDir", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.NOT_REQUIRED, 't', "The temporary directory to store intermediate files."),
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

        int batchSize = jsapResult.getInt("batchSize");
        File tempDir = jsapResult.contains("tempDir") ? new File(jsapResult.getString("tempDir")) : newBasenameDir;

        File clustersFile = newBasenameDir.toPath().resolve("clusters").toFile();

        logger.info("Loading simplified graph");
        ImmutableGraph graph = BVGraph.loadMapped(simplifiedBasename, pl);

        logger.info("Computing permutation");
        LayeredLabelPropagation llp = new LayeredLabelPropagation(graph, SEED);
        int[] permutation = llp.computePermutation(clustersFile.toString());

        llp = null;
        graph = null;

        logger.info("Loading original graph");
        graph = ImmutableGraph.loadOffline(basename, pl);

        logger.info("Applying permutation");
        graph = Transform.mapOffline(graph, permutation, batchSize, tempDir, pl);

        logger.info("Storing graph");
        BVGraph.store(graph, newBasenameDir.toPath().resolve(new File(simplifiedBasename).getName()).toString(), pl);
    }
}
