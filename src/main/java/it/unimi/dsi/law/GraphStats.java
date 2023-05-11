package it.unimi.dsi.law;

import com.martiansoftware.jsap.*;
import it.unimi.dsi.fastutil.longs.LongSet;
import it.unimi.dsi.logging.ProgressLogger;
import it.unimi.dsi.webgraph.labelling.ArcLabelledImmutableGraph;
import it.unimi.dsi.webgraph.labelling.ArcLabelledNodeIterator;
import it.unimi.dsi.webgraph.labelling.LongArrayListLabel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class GraphStats {
	private static final Logger logger = LoggerFactory.getLogger(GraphStats.class);
	private static final ProgressLogger pl = new ProgressLogger(logger);

	public static void main(String[] args) throws IOException, JSAPException {
		final SimpleJSAP jsap = new SimpleJSAP(GraphStats.class.getName(), "Compute some stats on the given transaction graph",
				new Parameter[]{
						new UnflaggedOption("basename", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, false, "The basename of the graph."),
				}
		);

		final JSAPResult jsapResult = jsap.parse(args);
		if (jsap.messagePrinted()) System.exit(1);

		String basename = jsapResult.getString("basename");
		ArcLabelledImmutableGraph g = ArcLabelledImmutableGraph.load(basename, pl);

		pl.start("Computing: label size mean, outdegree mean");
		pl.expectedUpdates = g.numNodes();

		float labelSizeMean = 0f;
		float outdegreeMean = 0f;

		float labelCount = 0f;
		float nodeCount = 0f;

		for (ArcLabelledNodeIterator it = g.nodeIterator(); it.hasNext();) {
			// int node = it.nextInt();
			outdegreeMean = (outdegreeMean * nodeCount) + it.outdegree() / (nodeCount + 1);

			for (LongArrayListLabel la: (LongArrayListLabel[]) it.labelArray()) {
				int unique = LongSet.of(la.values.toLongArray()).size();
				labelSizeMean = (labelSizeMean * labelCount) + unique / (labelCount + 1);
			}

			nodeCount++;
			labelCount++;

			pl.lightUpdate();
		}
	}
}
