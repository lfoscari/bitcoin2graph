package it.unimi.dsi.law;

import com.martiansoftware.jsap.*;
import it.unimi.dsi.fastutil.io.BinIO;
import it.unimi.dsi.io.FileLinesMutableStringIterable;
import it.unimi.dsi.lang.MutableString;
import it.unimi.dsi.logging.ProgressLogger;
import it.unimi.dsi.webgraph.ImmutableGraph;
import it.unimi.dsi.webgraph.NodeIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.Immutable;
import java.io.IOException;

public class LocalClusteringCoefficient {
	private static final Logger logger = LoggerFactory.getLogger(LocalClusteringCoefficient.class);
	private static final ProgressLogger pl = new ProgressLogger(logger);

	public static void main(String[] args) throws IOException, JSAPException {
		final SimpleJSAP jsap = new SimpleJSAP(LocalClusteringCoefficient.class.getName(), "Compute the local clustering coefficient for all the nodes in the given graphs according to the number of triangles," +
				"provided as a file where each row contains the id of a node and its number of triangles, separated by a single space. The results are stored in a file with the same basename with the" +
				"extension '-clustering.doubles' as an array of doubles.",
				new Parameter[]{
						new UnflaggedOption("basename", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, false, "The basename of the labelled transaction graph."),
						new UnflaggedOption("triangles", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, false, "The file containing the number of triangles for each node."),
				}
		);

		final JSAPResult jsapResult = jsap.parse(args);
		if (jsap.messagePrinted()) System.exit(1);

		final String basename = jsapResult.getString("basename");
		final String triangles = jsapResult.getString("triangles");
		final ImmutableGraph g = ImmutableGraph.load(basename, pl);

		final NodeIterator nodeIterator = g.nodeIterator();
		final FileLinesMutableStringIterable.FileLinesIterator nodeTriangles = new FileLinesMutableStringIterable(triangles).iterator();

		final double[] localClusteringCoefficient = new double[g.numNodes()];
		MutableString triangleLine;

		pl.start("Estimating local clustering coefficient");
		pl.expectedUpdates = g.numNodes();
		pl.itemsName = "nodes";

		while (nodeIterator.hasNext()) {
			final int node = nodeIterator.nextInt();
			final int outdegree = nodeIterator.outdegree();

			triangleLine = nodeTriangles.next();
			triangleLine.delete(0, triangleLine.indexOf(' ') + 1);

			final double triangleAmount = Double.parseDouble(triangleLine.toString());
			localClusteringCoefficient[node] = 2 * triangleAmount / (outdegree * (outdegree - 1));

			pl.lightUpdate();
		}
		pl.done();

		pl.logger.info("Storing results in " + basename + "-clustering.doubles...");
		BinIO.storeDoubles(localClusteringCoefficient, basename + "-clustering.doubles");

		double mean = 0;
		for (int node = 0; node < localClusteringCoefficient.length; node++)
			mean += (localClusteringCoefficient[node] - mean) / node;

		System.out.println("Global clustering coefficient: " + mean);
	}
}
