package it.unimi.dsi.law;

import com.martiansoftware.jsap.*;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.io.FastByteArrayOutputStream;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import it.unimi.dsi.io.OutputBitStream;
import it.unimi.dsi.logging.ProgressLogger;
import it.unimi.dsi.util.XoRoShiRo128PlusPlusRandom;
import it.unimi.dsi.webgraph.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;

public class GlobalClusteringCoefficient {
	private static final XoRoShiRo128PlusPlusRandom r = new XoRoShiRo128PlusPlusRandom();
	private static final Logger logger = LoggerFactory.getLogger(ClusteringCoefficient.class);
	private static final ProgressLogger pl = new ProgressLogger(logger);

	public static void main(String[] args) throws JSAPException, IOException {
		final SimpleJSAP jsap = new SimpleJSAP(GlobalClusteringCoefficient.class.getName(), "Compute the global clustering coefficient on the given graph",
				new Parameter[]{
						new FlaggedOption("batchSize", JSAP.INTSIZE_PARSER, Integer.toString(1000000), false, 's', "batch-size", "The maximum size of a batch, in arcs."),
						new UnflaggedOption("basename", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, false, "The basename of the graph (preferably symmetric)."),
				}
		);

		final JSAPResult jsapResult = jsap.parse(args);
		if (jsap.messagePrinted()) System.exit(1);

		String basename = jsapResult.getString("basename");
		ImmutableGraph g = ImmutableGraph.load(basename, pl);
		IntArrayList arcs = new IntArrayList(g.numNodes());

		// idea: costruisco un nuovo grafo
		// partendo dal grafo G
		// per ogni nodo n:
		//  per ogni vicino v del nodo n:
		//      per ogni vicino w del nodo n tranne v:
		//          aggiungi al nuovo grafo l'arco (v, w)
		// finisci di costruire il grafo
		// chiama G' il nuovo grafo
		// conta archi in G' mancano in G
		// ogni arco che è sia in G' che in G è una tripletta chiusa
		// ogni arco che non è in G' ma non in G è una tripletta aperta
		// calcola il numero totale di triplette aprte sommand k(v)(k(v) - 1) per k(v) outdegree di v

		int batchSize = jsapResult.getInt("batchSize");
		long pairs = 0;

		int[] source = new int[batchSize];
		int[] target = new int[batchSize];
		int j = 0;

		FastByteArrayOutputStream fbos = new FastByteArrayOutputStream();
		OutputBitStream obs = new OutputBitStream(fbos);
		ObjectArrayList<File> batches = new ObjectArrayList<>();

		pl.itemsName = "arcs";
		pl.expectedUpdates = g.numNodes();
		pl.start("Creating sorted batches...");

		NodeIterator nodeIterator = g.nodeIterator();
		while (nodeIterator.hasNext()) {
			nodeIterator.nextInt();
			int outdegree = nodeIterator.outdegree();
			int[] neighbours = nodeIterator.successorArray();

			for (int u = 0; u < outdegree; u++) {
				for (int v = u + 1; v < outdegree; v++) {
					if (neighbours[u] == neighbours[v]) continue;

					if (j >= batchSize) {
						pairs += Transform.processBatch(batchSize, source, target, null, batches);
						j = 0;
					}

					source[j] = neighbours[u];
					target[j] = neighbours[v];
					j++;

				}
			}

			pl.lightUpdate();
		}

		if (j != 0) pairs += Transform.processBatch(j, source, target, null, batches);
		pl.done();

		source = null;
		target = null;

		Transform.BatchGraph batchGraph = new Transform.BatchGraph(g.numNodes(), pairs, batches);
		BVGraph.store(batchGraph, basename + "-triangles", pl);
	}
}
