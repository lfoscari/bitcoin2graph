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

		int trials = 10000, triangles = 0;

		pl.itemsName = "nodes";
		pl.displayFreeMemory = true;

		pl.start("Picking " + trials + " random nodes");
		int[] nodes = r.ints(0, g.numNodes()).distinct().limit(trials).toArray();
		IntArrays.unstableSort(nodes);
		pl.done();

		int[][] triangleNodes = buildNodePairs(trials, g.nodeIterator(), nodes);
		float globalClusteringCoefficient = countTriangles(g, trials, triangles, triangleNodes);

		System.out.println(globalClusteringCoefficient);
	}

	private static int[][] buildNodePairs(int trials, NodeIterator nodeIterator, int[] nodes) {
		pl.start("Building node pairs");
		pl.expectedUpdates = trials;

		int[][] triangleNodes = new int[trials][2];

		int previous = 0;
		for (int i = 0; i < trials; i++) {
			int sourceNode = nodes[i];
			nodeIterator.skip(sourceNode - previous - 1);
			assert nodeIterator.nextInt() == sourceNode;

			if (nodeIterator.outdegree() < 2) {
				// ???
				continue;
			}

			int[] neighbours = nodeIterator.successorArray();
			int[] chosenIndices = r.ints(0, nodeIterator.outdegree()).distinct().limit(2).toArray();

			triangleNodes[i][0] = neighbours[chosenIndices[0]];
			triangleNodes[i][1] = neighbours[chosenIndices[1]];

			previous = sourceNode;
			pl.lightUpdate();
		}

		ObjectArrays.unstableSort(triangleNodes, Comparator.comparingInt(a -> a[1]));

		pl.stop();
		return triangleNodes;
	}

	private static float countTriangles(ImmutableGraph g, int trials, int triangles, int[][] triangleNodes) {
		pl.start("Counting triangles");
		pl.expectedUpdates = trials;

		NodeIterator it = g.nodeIterator(triangleNodes[0][0]);
		int previous = 0;
		for (int i = 0; i < trials; i++) {
			int sourceNode = triangleNodes[i][0];
			int destNode = triangleNodes[i][1];

			it.skip(sourceNode - previous - 1);
			assert it.nextInt() == sourceNode;

			if (ArrayUtils.contains(it.successorArray(), destNode)) {
				triangles++;
			}

			previous = sourceNode;
			pl.lightUpdate();
		}

		pl.done();
		return (float) triangles / trials;
	}
}
