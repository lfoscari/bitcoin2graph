package it.unimi.dsi.law;

import com.martiansoftware.jsap.*;
import it.unimi.dsi.fastutil.ints.IntArrays;
import it.unimi.dsi.fastutil.io.TextIO;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import it.unimi.dsi.logging.ProgressLogger;
import it.unimi.dsi.webgraph.labelling.ArcLabelledImmutableGraph;
import it.unimi.dsi.webgraph.labelling.ArcLabelledNodeIterator;
import it.unimi.dsi.webgraph.labelling.Label;
import it.unimi.dsi.webgraph.labelling.MergeableFixedWidthLongListLabel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;

public class TransactionDegree {
	private static final Logger logger = LoggerFactory.getLogger(TransactionDegree.class);
	private static final ProgressLogger pl = new ProgressLogger(logger);

	public static void main(String[] args) throws IOException, JSAPException {
		final SimpleJSAP jsap = new SimpleJSAP(TransactionDegree.class.getName(), "Compute for each address the number of transactions in which it was involved.",
				new Parameter[]{
						new UnflaggedOption("basename", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, false, "The basename of the labelled transaction graph."),
						new UnflaggedOption("basenameTransposed", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, false, "The basename of the transposed of the labelled transaction graph."),
						new UnflaggedOption("outputBasename", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.NOT_REQUIRED, false, "The optional basename of the output files storing the cardinalities for the amounts of inputs and outputs where at line x there is the number of nodes with x in/outputs."),
				}
		);

		final JSAPResult jsapResult = jsap.parse(args);
		if (jsap.messagePrinted()) System.exit(1);

		final ArcLabelledImmutableGraph graph = ArcLabelledImmutableGraph.load(jsapResult.getString("basename"), pl);
		pl.start("Computing transaction inputs cardinality");
		final int[] transactionInput = transactionsPerNode(graph);
		pl.done();

		final ArcLabelledImmutableGraph transposed = ArcLabelledImmutableGraph.load(jsapResult.getString("basenameTransposed"), pl);
		pl.start("Computing transaction outputs cardinality");
		final int[] transactionOutput = transactionsPerNode(transposed);
		pl.done();

		System.out.println(Arrays.equals(transactionInput, transactionOutput));
		System.out.println("Average inputs per node: " + mean(transactionInput));
		System.out.println("Average outputs per node: " + mean(transactionOutput));

		System.out.println("Addresses with one input: " + onePercentage(transactionInput) + "%");
		System.out.println("Addresses with one output: " + onePercentage(transactionOutput) + "%");

		if (jsapResult.contains("outputBasename")) {
			pl.start("Computing counts for the cardinalities");

			int[] inputCardinality = computeCardinalities(transactionInput);
			TextIO.storeInts(inputCardinality, jsapResult.getString("outputBasename") + ".input");

			int[] outputCardinality = computeCardinalities(transactionOutput);
			TextIO.storeInts(outputCardinality, jsapResult.getString("outputBasename") + ".output");

			pl.done();
		}
	}

	private static double onePercentage(int[] transactionData) {
		double ones = 0;
		for (int n: transactionData) if (n == 1) ones++;
		return (ones / transactionData.length) * 100;
	}

	private static int[] transactionsPerNode(ArcLabelledImmutableGraph graph) {
		pl.expectedUpdates = graph.numNodes();
		pl.itemsName = "nodes";

		final int[] transactionAmount = new int[graph.numNodes()];
		final ArcLabelledNodeIterator it = graph.nodeIterator();
		LongOpenHashSet transactionIds = new LongOpenHashSet();

		for (int i = 0; i < transactionAmount.length; i++) {
			transactionIds.clear();
			final int node = it.nextInt();
			final Label[] labels = it.labelArray();

			for (int j = 0; j < it.outdegree(); j++) {
				for (long transactionId: ((MergeableFixedWidthLongListLabel) labels[j]).value)
					transactionIds.add(transactionId);
			}

			transactionAmount[node] = transactionIds.size();
			pl.lightUpdate();
		}

		return transactionAmount;
	}

	private static double mean(final int[] transactionData) {
		double sum = 0;
		for (int in: transactionData) sum += in;
		return sum / transactionData.length;
	}

	private static int[] computeCardinalities(final int[] transactionData) {
		int[] cardinality = IntArrays.EMPTY_ARRAY;
		int maxd = 0;

		for (final int d : transactionData) {
			if (d >= cardinality.length) cardinality = IntArrays.grow(cardinality, d + 1);
			if (d > maxd) maxd = d;
			cardinality[d]++;
		}

		return IntArrays.trim(cardinality, maxd + 1);
	}
}