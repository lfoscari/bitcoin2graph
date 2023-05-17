package it.unimi.dsi.law;

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import com.martiansoftware.jsap.*;
import it.unimi.dsi.fastutil.ints.Int2BooleanFunction;
import it.unimi.dsi.fastutil.objects.ObjectArrays;
import it.unimi.dsi.logging.ProgressLogger;
import it.unimi.dsi.util.XoRoShiRo128PlusPlusRandom;
import it.unimi.dsi.webgraph.ImmutableGraph;
import it.unimi.dsi.webgraph.NodeIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Comparator;

public class ClusteringCoefficient {
	private static final XoRoShiRo128PlusPlusRandom r = new XoRoShiRo128PlusPlusRandom();
	private static final Logger logger = LoggerFactory.getLogger(ClusteringCoefficient.class);
	private static final ProgressLogger pl = new ProgressLogger(logger);

	public static void main(String[] args) throws IOException, JSAPException {
		final SimpleJSAP jsap = new SimpleJSAP(ClusteringCoefficient.class.getName(), "Compute the clustering coefficient on the given graph",
				new Parameter[]{
						new FlaggedOption("samplingFactor", JSAP.DOUBLE_PARSER, "0.3", JSAP.NOT_REQUIRED, 's', "The sampling factor (default: 0.3)."),
						new UnflaggedOption("basename", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, false, "The basename of the graph."),
				}
		);

		final JSAPResult jsapResult = jsap.parse(args);
		if (jsap.messagePrinted()) System.exit(1);
		
		String basename = jsapResult.getString("basename");
		double samplingFactor = jsapResult.getDouble("samplingFactor");

		ImmutableGraph g = ImmutableGraph.load(basename, pl);

		pl.itemsName = "nodes";
		pl.displayFreeMemory = true;
		
		// Build a filter to sample {samplingFactor} of the nodes (a better but more memory-
		// consuming solution would be a random permutation int[])

		Int2BooleanFunction nodeFilter = new Int2BooleanFunction() {
			private final HashFunction hf = Hashing.adler32();
			private final int threshold = (int) (samplingFactor * Integer.MAX_VALUE);

			@Override
			public boolean get(int key) {
				return this.hf.hashInt(key).asInt() < this.threshold;
			}
		};

		int[][] nodePairs = collectNodePairs(g.nodeIterator(), g.numNodes(), samplingFactor, nodeFilter);
		pl.logger.info("Sampled " + nodePairs.length + "/" + g.numNodes() + " nodes (" + ((float) nodePairs.length / g.numNodes()) * 100 + "%)");
		float globalClusteringCoefficient = countTriangles(g.nodeIterator(), nodePairs);

		System.out.println(globalClusteringCoefficient);
	}

	private static int[][] collectNodePairs(NodeIterator nodeIterator, int numNodes, double samplingFactor, Int2BooleanFunction nodeFilter) {
		pl.start("Sampling node pairs");
		pl.expectedUpdates = numNodes;

		int index = 0;
		int[][] triangleNodes = new int[(int) (numNodes * samplingFactor)][2];

		while (nodeIterator.hasNext()) {
			int node = nodeIterator.nextInt();

			if (nodeIterator.outdegree() < 2 || !nodeFilter.get(node))
				continue;

			int[] neighbours = nodeIterator.successorArray();

			// Generate two distinct indices in [0..outdegree]
			int firstIndex = r.nextInt(nodeIterator.outdegree());
			int secondIndex;
			while ((secondIndex = r.nextInt(nodeIterator.outdegree())) == firstIndex);

			if (index >= triangleNodes.length)
				break; // out of space

			triangleNodes[index++] = new int[] { neighbours[firstIndex], neighbours[secondIndex] };
			pl.lightUpdate();
		}

		ObjectArrays.trim(triangleNodes, index);
		ObjectArrays.unstableSort(triangleNodes, Comparator.comparingInt(a -> a[0]));

		pl.done();
		return triangleNodes;
	}

	private static float countTriangles(NodeIterator nodeIterator, int[][] nodePairs) {
		pl.start("Counting connected node pairs");
		pl.expectedUpdates = nodePairs.length;
		pl.itemsName = "pairs";

		int connected = 0;
		for (int[] nodePair : nodePairs) {
			int sourceNode = nodePair[0], destNode = nodePair[1];
			while (nodeIterator.nextInt() < sourceNode);

			if (contains(nodeIterator.successorArray(), destNode, nodeIterator.outdegree()))
				connected++;
			pl.lightUpdate();
		}

		pl.done();
		return (float) connected / nodePairs.length;
	}

	private static boolean contains(int[] array, int valueToFind, int length) {
		for (int i = 0; i < Math.min(array.length, length); i++)
			if (array[i] == valueToFind) return true;
		return false;
	}
}
