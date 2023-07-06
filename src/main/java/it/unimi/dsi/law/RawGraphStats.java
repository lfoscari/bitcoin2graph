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

		final String basename = jsapResult.getString("basename");

		final Properties p = new Properties();
		p.load(Files.newInputStream(Paths.get(basename + ".properties")));

		// Get long width from labelspec
		final String labelSpec = p.getProperty("labelspec");
		final String labelSize = labelSpec.substring(labelSpec.lastIndexOf(',') + 1, labelSpec.length() - 1);
		final int transactionSize = Integer.parseInt(labelSize);

		final String underlyingBasename = new File(basename).getParentFile().toPath().resolve(p.getProperty("underlyinggraph")).toString();
		final ImmutableGraph g = ImmutableGraph.loadOffline(underlyingBasename);
		final int[] transactionAmount = new int[g.numNodes()];

		final File labelFile = new File(basename + ".labels");
		final InputBitStream fbis = new InputBitStream(Files.newInputStream(labelFile.toPath()));

		pl.start("Computing transaction amount for each node");
		pl.itemsName = "nodes";
		pl.expectedUpdates = g.numNodes();

		int length;
		for (int i = 0; i < transactionAmount.length; i++) {
			try {
				length = fbis.readGamma();
			} catch (EOFException e) {
				break;
			}

			transactionAmount[i] = length;

			// Each transaction is {transactionSize} long for a total of:
			final long size = (long) length * transactionSize;
			fbis.skip(size);

			pl.lightUpdate();
		}
		pl.done();

		if (fbis.hasNext()) pl.logger.info("The label file still had " + fbis.skip(Long.MAX_VALUE) + " bits");
		fbis.close();

		BinIO.storeInts(transactionAmount, jsapResult.getFile("output"));
	}
}
