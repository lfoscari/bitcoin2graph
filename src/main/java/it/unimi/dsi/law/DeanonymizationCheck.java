package it.unimi.dsi.law;

import com.martiansoftware.jsap.*;
import it.unimi.dsi.fastutil.ints.IntArrays;
import it.unimi.dsi.io.FileLinesMutableStringIterable;
import it.unimi.dsi.lang.MutableString;
import it.unimi.dsi.logging.ProgressLogger;
import it.unimi.dsi.util.XoRoShiRo128PlusPlusRandom;
import it.unimi.dsi.webgraph.ImmutableGraph;
import it.unimi.dsi.webgraph.NodeIterator;
import org.apache.commons.lang3.ArrayUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;

public class DeanonymizationCheck {
	private static final Logger logger = LoggerFactory.getLogger(DeanonymizationCheck.class);
	private static final ProgressLogger pl = new ProgressLogger(logger);

	public static void main(String[] args) throws JSAPException, IOException, ClassNotFoundException {
		final SimpleJSAP jsap = new SimpleJSAP(DeanonymizationCheck.class.getName(), "Deanonymization.", new Parameter[] {new FlaggedOption("addresses", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, 'o', "A file with all the addresses in string form."), new FlaggedOption("seed", JSAP.LONG_PARSER, "123798", JSAP.REQUIRED, 's', "Random seed."), new UnflaggedOption("basename", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.NOT_REQUIRED, false, "Basename for the graph.")});

		final JSAPResult jsapResult = jsap.parse(args);
		if (jsap.messagePrinted()) System.exit(1);

		final XoRoShiRo128PlusPlusRandom r = new XoRoShiRo128PlusPlusRandom(jsapResult.getLong("seed"));
		ImmutableGraph g = ImmutableGraph.load(jsapResult.getString("basename"), pl);

		NodeIterator nodeIt = g.nodeIterator(r.nextInt(g.numNodes()));
		int node = nodeIt.nextInt();
		int[] successors = nodeIt.successorArray();

		System.out.println(node + ": " + Arrays.toString(successors));

		int[] nodes = IntArrays.grow(successors, successors.length + 1);
		nodes[nodes.length - 1] = node;

		try (FileLinesMutableStringIterable.FileLinesIterator addrIt = new FileLinesMutableStringIterable(jsapResult.getString("addresses")).iterator()) {
			MutableString address;
			for (int addressId = 0; addressId < g.numNodes(); addressId++) {
				address = addrIt.next();

				int pos = ArrayUtils.indexOf(nodes, addressId);
				if (pos == ArrayUtils.INDEX_NOT_FOUND) continue;

				System.out.println(nodes[pos] + " => " + address + " (" + addressId + ")");
			}
		}
	}
}
