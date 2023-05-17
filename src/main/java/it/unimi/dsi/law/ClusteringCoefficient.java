package it.unimi.dsi.law;

import com.martiansoftware.jsap.*;
import it.unimi.dsi.fastutil.ints.IntArrays;
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

	private static final int trials = 10000;

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

		pl.start("Picking " + trials + " random nodes");
		int[] nodes = r.ints(0, g.numNodes() / 10).distinct().limit(trials).toArray();
		IntArrays.unstableSort(nodes);
		pl.done();

		int[][] triangleNodes = buildNodePairs(trials, g.nodeIterator(), nodes);
		float globalClusteringCoefficient = countTriangles(g, g.nodeIterator(), triangleNodes);

		System.out.println(globalClusteringCoefficient);
	}

	private static int[][] buildNodePairs(int trials, NodeIterator nodeIterator, int[] nodes) {
		pl.start("Building node pairs");
		pl.expectedUpdates = trials;

		int[][] triangleNodes = new int[trials][2];

		for (int i = 0; i < trials; i++) {
			int sourceNode = nodes[i];
			
			while (nodeIterator.nextInt() != sourceNode);
			if (nodeIterator.outdegree() < 2)
				continue; // ???
			
			int[] neighbours = nodeIterator.successorArray();
			int[] chosenIndices = r.ints(0, nodeIterator.outdegree()).distinct().limit(2).toArray();

			triangleNodes[i][0] = neighbours[chosenIndices[0]];
			triangleNodes[i][1] = neighbours[chosenIndices[1]];

			pl.lightUpdate();
		}

		ObjectArrays.unstableSort(triangleNodes, Comparator.comparingInt(a -> a[0]));

		pl.done();
		return triangleNodes;
	}

	private static float countTriangles(ImmutableGraph g, NodeIterator nodeIterator, int[][] triangleNodes) {
		pl.start("Counting triangles");
		pl.expectedUpdates = triangleNodes.length;

		int triangles = 0;

		for (int i = 0; i < triangleNodes.length; i++) {
			int sourceNode = triangleNodes[i][0], destNode = triangleNodes[i][1];

			int node;
			while ((node = nodeIterator.nextInt()) < sourceNode);
			
			if (node > sourceNode) {
				// sourceNode has no neighbours
				continue;
			}
			
			if (ArrayUtils.contains(nodeIterator.successorArray(), destNode))
				triangles++;
			
			pl.lightUpdate();
		}

		pl.done();
		return (float) triangles / trials;
	}
}
