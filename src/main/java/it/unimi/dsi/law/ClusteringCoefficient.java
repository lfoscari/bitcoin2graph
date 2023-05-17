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
import org.apache.commons.lang3.ArrayUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Comparator;

public class ClusteringCoefficient {
	private static final XoRoShiRo128PlusPlusRandom r = new XoRoShiRo128PlusPlusRandom();
	private static final Logger logger = LoggerFactory.getLogger(ClusteringCoefficient.class);
	private static final ProgressLogger pl = new ProgressLogger(logger);

	private static final float samplingFactor = 30.0F / 100.0F;

	public static void main(String[] args) throws IOException, JSAPException {
		final SimpleJSAP jsap = new SimpleJSAP(ClusteringCoefficient.class.getName(), "Compute the clustering coefficient on the given graph",
				new Parameter[]{
						new UnflaggedOption("basename", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, false, "The basename of the graph."),
				}
		);

		final JSAPResult jsapResult = jsap.parse(args);
		if (jsap.messagePrinted()) System.exit(1);
		
		String basename = jsapResult.getString("basename");
		ImmutableGraph g = ImmutableGraph.load(basename, pl);

		pl.itemsName = "nodes";
		pl.displayFreeMemory = true;
		
		// Build a hashing function with 100 outputs and create a filter passing only
		// values with a value of at most samplingFactor * 100 (a better much more memory-
		// consuming solution would be a random permutation int[])

		Int2BooleanFunction nodeFilter = new Int2BooleanFunction() {
			private final HashFunction hf = Hashing.adler32();

			@Override
			public boolean get(int key) {
				return (this.hf.hashInt(key).asInt() % 100) < (samplingFactor * 100);
			}
		};

		int[][] nodePairs = collectNodePairs(g.nodeIterator(), (int) (g.numNodes() * samplingFactor), nodeFilter);
		pl.logger.info("Sampled " + nodePairs.length + "/" + g.numNodes() + " nodes (" + ((float) nodePairs.length / g.numNodes()) * 100 + "%)");
		float globalClusteringCoefficient = countTriangles(g.nodeIterator(), nodePairs);

		System.out.println(globalClusteringCoefficient);
	}

	private static int[][] collectNodePairs(NodeIterator nodeIterator, int nodesToSample, Int2BooleanFunction nodeFilter) {
		pl.start("Building node pairs");
		pl.expectedUpdates = (int) (nodesToSample / samplingFactor);

		int index = 0;
		int[][] triangleNodes = new int[nodesToSample][2];

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

			if (ArrayUtils.contains(nodeIterator.successorArray(), destNode))
				connected++;

			pl.lightUpdate();
		}

		pl.done();
		return (float) connected / nodePairs.length;
	}
}
