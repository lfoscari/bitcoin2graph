package it.unimi.dsi.law;

import com.martiansoftware.jsap.*;
import it.unimi.dsi.fastutil.longs.LongArraySet;
import it.unimi.dsi.fastutil.longs.LongCollection;
import it.unimi.dsi.fastutil.longs.LongList;
import it.unimi.dsi.fastutil.longs.LongSet;
import it.unimi.dsi.logging.ProgressLogger;
import it.unimi.dsi.webgraph.labelling.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

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
		BitStreamArcLabelledImmutableGraph g = BitStreamArcLabelledImmutableGraph.load(basename, pl);

		pl.start("Computing: label size mean, outdegree mean");
		pl.expectedUpdates = g.numNodes();
		pl.itemsName = "nodes";
		pl.logInterval = TimeUnit.SECONDS.toMillis(10);

		float labelSizeMean = 0f;
		float outdegreeMean = 0f;

		float labelCount = 0f;
		float nodeCount = 0f;

		for (ArcLabelledNodeIterator it = g.nodeIterator(); it.hasNext(); ) {
			it.nextInt();
			outdegreeMean = (outdegreeMean * nodeCount) + it.outdegree() / (nodeCount + 1);

			for (Label la: it.labelArray()) {
				LongCollection values = ((LongArrayListLabel) la).values;
				int unique = new LongArraySet(values).size();
				labelSizeMean = (labelSizeMean * labelCount) + unique / (labelCount + 1);
			}

			nodeCount++;
			labelCount++;

			pl.lightUpdate();
		}

		System.out.println("mean label size: " + labelSizeMean);
		System.out.println("mean outdegree: " + outdegreeMean);
	}
}
