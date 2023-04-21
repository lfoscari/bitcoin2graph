package it.unimi.dsi.law;

import com.martiansoftware.jsap.*;
import it.unimi.dsi.fastutil.io.FastBufferedOutputStream;
import it.unimi.dsi.io.FileLinesMutableStringIterable;
import it.unimi.dsi.logging.ProgressLogger;
import it.unimi.dsi.webgraph.ImmutableGraph;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Paths;

public class NeighbourhoodPlot {
    private static final Logger logger = LoggerFactory.getLogger(Simplify.class);
    private static final ProgressLogger pl = new ProgressLogger(logger);

    public static void main(String[] args) throws JSAPException, IOException {
        final SimpleJSAP jsap = new SimpleJSAP(NeighbourhoodPlot.class.getName(),
                "Read a neighbourhood function and save to a file the values normalized by the amount of nodes in the associated graph.",
                new Parameter[] {new UnflaggedOption("functionFile", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, false, "The function file.")}
        );

        final JSAPResult jsapResult = jsap.parse(args);
        if (jsap.messagePrinted()) System.exit(1);

        String ff = jsapResult.getString("functionFile");
        if (!new File(ff).exists()) throw new JSAPException("File " + ff + " does not exist!");

        FileLinesMutableStringIterable msit = new FileLinesMutableStringIterable(ff);
        pl.start("Reading neighbourhood function");

        try (PrintStream normalized = new PrintStream(new FastBufferedOutputStream(Files.newOutputStream(Paths.get(ff + ".normalized"))));
             PrintStream singleValues = new PrintStream(new FastBufferedOutputStream(Files.newOutputStream(Paths.get(ff + ".single"))));
             FileLinesMutableStringIterable.FileLinesIterator iterator = msit.iterator()) {

            double numNodes = Double.parseDouble(iterator.next().toString());
            double squaredNumNodes = numNodes * numNodes;

            normalized.println(1);
            singleValues.println(1);

            double previous = 1;
            while (iterator.hasNext()) {
                double pairAmount = Double.parseDouble(iterator.next().toString());
                normalized.println(pairAmount / squaredNumNodes);
                singleValues.println((pairAmount / numNodes) - previous);

                previous = (pairAmount / numNodes) - previous;
                pl.lightUpdate();
            }
        }

        pl.done();
    }
}
