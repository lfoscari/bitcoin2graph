package it.unimi.dsi.law;

import com.martiansoftware.jsap.*;
import it.unimi.dsi.logging.ProgressLogger;
import it.unimi.dsi.webgraph.BVGraph;
import it.unimi.dsi.webgraph.ImmutableGraph;
import it.unimi.dsi.webgraph.Transform;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;

import static it.unimi.dsi.webgraph.Transform.NO_LOOPS;

public class Transpose {
    private static final Logger logger = LoggerFactory.getLogger(Compress.class);
    private static final ProgressLogger pl = new ProgressLogger(logger);

    public static void main(String[] args) throws JSAPException, IOException {
        final SimpleJSAP jsap = new SimpleJSAP(Compress.class.getName(), "Transpose a given graph",
                new Parameter[] {
                        new FlaggedOption("batchSize", JSAP.INTEGER_PARSER, "10000000", JSAP.NOT_REQUIRED, 'b', "The batch size."),
                        new FlaggedOption("tempDir", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.NOT_REQUIRED, 't', "The temporary directory to store intermediate files."),
                        new FlaggedOption("destBasename", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.NOT_REQUIRED, 'd', "The basename of the resulting graph."),
                        new UnflaggedOption("basename", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, false, "The basename of the graph."),
                }
        );

        final JSAPResult jsapResult = jsap.parse(args);
        if (jsap.messagePrinted()) System.exit(1);

        Path basenamePath = new File(jsapResult.getString("basename")).toPath();
        File destBasename = jsapResult.contains("destBasename") ?
                new File(jsapResult.getString("destBasename")) :
                basenamePath.getParent().resolve("transposed").resolve(basenamePath.getFileName()).toFile();


        if (!destBasename.getParentFile().exists()) {
            logger.warn(destBasename.getParentFile() + " does not exist, creating it");
            destBasename.getParentFile().mkdir();
        }

        int batchSize = jsapResult.getInt("batchSize");
        File tempDir = jsapResult.contains("tempDir") ? new File(jsapResult.getString("tempDir")) : basenamePath.getParent().toFile();

        ImmutableGraph graph = ImmutableGraph.loadOffline(basenamePath.toString(), pl);
        graph = Transform.transposeOffline(graph, batchSize, tempDir, pl);

        BVGraph.store(graph, destBasename.toString(), pl);
    }
}
