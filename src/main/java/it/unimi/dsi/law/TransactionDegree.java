package it.unimi.dsi.law;

import com.martiansoftware.jsap.*;
import it.unimi.dsi.fastutil.io.BinIO;
import it.unimi.dsi.fastutil.io.FastBufferedOutputStream;
import it.unimi.dsi.logging.ProgressLogger;
import it.unimi.dsi.webgraph.ImmutableGraph;
import it.unimi.dsi.webgraph.labelling.ArcLabelledImmutableGraph;
import it.unimi.dsi.webgraph.labelling.ArcLabelledNodeIterator;
import it.unimi.dsi.webgraph.labelling.Label;
import it.unimi.dsi.webgraph.labelling.MergeableFixedWidthLongListLabel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
						new UnflaggedOption("outputFile", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.NOT_REQUIRED, false, "The output file."),
				}
		);

		final JSAPResult jsapResult = jsap.parse(args);
		if (jsap.messagePrinted()) System.exit(1);

		final ArcLabelledImmutableGraph g = ArcLabelledImmutableGraph.load(jsapResult.getString("basename"), pl);
		final int[] transactionInput = new int[g.numNodes()], transactionOutput = new int[g.numNodes()];

		pl.start("Computing transaction cardinality");
		pl.expectedUpdates = g.numNodes();
		pl.itemsName = "nodes";

		for (final ArcLabelledNodeIterator it = g.nodeIterator(); it.hasNext(); ) {
			final int node = it.nextInt();
			final int[] neighbours = it.successorArray();
			final Label[] labels = it.labelArray();

			int outputAmount = 0;
			for (int i = 0; i < it.outdegree(); i++) {
				final int length = ((MergeableFixedWidthLongListLabel) labels[i]).value.length;
				outputAmount += length;
				transactionInput[neighbours[i]] += length;
			}

			transactionOutput[node] = outputAmount;
			pl.lightUpdate();
		}

		pl.done();

		try (FastBufferedOutputStream fbos = new FastBufferedOutputStream(Files.newOutputStream(Paths.get(jsapResult.getString("outputFile"))))) {
			for (int i = 0; i < transactionInput.length; i++) {
				fbos.write(transactionInput[i]);
				fbos.write('\t');
				fbos.write(transactionOutput[i]);
				fbos.write('\n');
			}
		}
	}
}