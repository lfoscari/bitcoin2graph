package it.unimi.dsi.law;

import com.martiansoftware.jsap.*;
import it.unimi.dsi.fastutil.ints.IntArrays;
import it.unimi.dsi.fastutil.io.BinIO;
import it.unimi.dsi.fastutil.io.FastBufferedOutputStream;
import it.unimi.dsi.fastutil.io.TextIO;
import it.unimi.dsi.logging.ProgressLogger;
import it.unimi.dsi.webgraph.ImmutableGraph;
import it.unimi.dsi.webgraph.labelling.ArcLabelledImmutableGraph;
import it.unimi.dsi.webgraph.labelling.ArcLabelledNodeIterator;
import it.unimi.dsi.webgraph.labelling.Label;
import it.unimi.dsi.webgraph.labelling.MergeableFixedWidthLongListLabel;
import org.checkerframework.common.value.qual.IntRange;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Text;

import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

public class TransactionDegree {
	private static final Logger logger = LoggerFactory.getLogger(TransactionDegree.class);
	private static final ProgressLogger pl = new ProgressLogger(logger);

	public static void main(String[] args) throws IOException, JSAPException {
		final SimpleJSAP jsap = new SimpleJSAP(TransactionDegree.class.getName(), "Compute for each address the number of transactions in which it was involved.",
				new Parameter[]{
						new UnflaggedOption("basename", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, false, "The basename of the labelled transaction graph."),
						new UnflaggedOption("outputBasename", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, false, "The basename of the output files."),
				}
		);

		final JSAPResult jsapResult = jsap.parse(args);
		if (jsap.messagePrinted()) System.exit(1);

		final ArcLabelledImmutableGraph g = ArcLabelledImmutableGraph.load(jsapResult.getString("basename"), pl);
		final int[] transactionInput = new int[g.numNodes()], transactionOutput = new int[g.numNodes()];

		pl.start("Computing transaction cardinality");
		pl.expectedUpdates = g.numNodes();
		pl.itemsName = "nodes";

		final ArcLabelledNodeIterator it = g.nodeIterator();
		for (int i = 0; i < g.numNodes(); i++) {
			final int node = it.nextInt();
			final int[] neighbours = it.successorArray();
			final Label[] labels = it.labelArray();

			int outputAmount = 0;
			for (int j = 0; j < it.outdegree(); j++) {
				final int length = ((MergeableFixedWidthLongListLabel) labels[j]).value.length;
				outputAmount += length;
				transactionInput[neighbours[j]] += length;
			}

			transactionOutput[node] = outputAmount;
			pl.lightUpdate();
		}

		pl.done();

		pl.start("Computing counts for the cardinalities");

		int[] inputCardinality = IntArrays.EMPTY_ARRAY;
		int maxOutdegree = computeCardinalities(transactionInput, inputCardinality);
		TextIO.storeInts(inputCardinality, 0, maxOutdegree + 1,jsapResult.getString("outputBasename") + ".input");

		int[] outputCardinality = IntArrays.EMPTY_ARRAY;
		maxOutdegree = computeCardinalities(transactionOutput, outputCardinality);
		TextIO.storeInts(outputCardinality, 0, maxOutdegree + 1, jsapResult.getString("outputBasename") + ".output");

		pl.done();
	}

	private static int computeCardinalities(final int[] transactionData, int[] cardinality) {
		int maxd = 0;
		for (final int j : transactionData) {
			if (j >= cardinality.length) cardinality = IntArrays.grow(cardinality, j + 1);
			if (j > maxd) maxd = j;
			cardinality[j]++;
		}
		return maxd;
	}
}