package it.unimi.dsi.law;

import com.martiansoftware.jsap.*;
import it.unimi.dsi.fastutil.io.BinIO;
import it.unimi.dsi.fastutil.longs.LongArrays;
import it.unimi.dsi.io.InputBitStream;
import it.unimi.dsi.logging.ProgressLogger;
import it.unimi.dsi.webgraph.ImmutableGraph;
import it.unimi.dsi.webgraph.labelling.ArcLabelledImmutableGraph;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.lang.module.Configuration;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class RawGraphStats {
	private static final Logger logger = LoggerFactory.getLogger(RawGraphStats.class);
	private static final ProgressLogger pl = new ProgressLogger(logger);

	public static void main(String[] args) throws IOException, JSAPException {
		final SimpleJSAP jsap = new SimpleJSAP(RawGraphStats.class.getName(), "Compute some stats on the given transaction graph",
				new Parameter[]{
						new UnflaggedOption("basename", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, false, "The basename of the graph."),
						new UnflaggedOption("output", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.NOT_REQUIRED, false, "The output file."),
				}
		);

		final JSAPResult jsapResult = jsap.parse(args);
		if (jsap.messagePrinted()) System.exit(1);

		String basename = jsapResult.getString("basename");

		Properties p = new Properties();
		p.load(Files.newInputStream(Paths.get(basename + ".properties")));

		// Get long width from labelspec
		String labelSpec = p.getProperty("labelspec");
		String labelSize = labelSpec.substring(labelSpec.lastIndexOf(',') + 1, labelSpec.length() - 1);
		int transactionSize = Integer.parseInt(labelSize);

		String underlyingBasename = new File(basename).getParentFile().toPath().resolve(p.getProperty("underlyinggraph")).toString();
		ImmutableGraph g = ImmutableGraph.loadOffline(underlyingBasename);
		int[] transactionAmount = new int[g.numNodes()];

		File labelFile = new File(basename + ".labels");
		InputBitStream fbis = new InputBitStream(Files.newInputStream(labelFile.toPath()));

		pl.start("Computing transaction amount for each node");
		pl.itemsName = "nodes";
		pl.expectedUpdates = g.numNodes();

		int length, i = 0;
		while (fbis.hasNext()) {
			try {
				length = fbis.readGamma();
			} catch (EOFException e) {
				break;
			}

			transactionAmount[i] = length;

			// Each transaction is {transactionSize} long for a total of:
			long size = (long) length * transactionSize;
			fbis.skip(size);

			pl.lightUpdate();
			i++;
		}
		pl.done();

		BinIO.storeInts(transactionAmount, jsapResult.getFile("output"));
	}
}
