package it.unimi.dsi.law;

/*
 * Copyright (C) 2010-2020 Paolo Boldi, Massimo Santini and Sebastiano Vigna
 *
 *  This program is free software; you can redistribute it and/or modify it
 *  under the terms of the GNU General Public License as published by the Free
 *  Software Foundation; either version 3 of the License, or (at your option)
 *  any later version.
 *
 *  This program is distributed in the hope that it will be useful, but
 *  WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY
 *  or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License
 *  for more details.
 *
 *  You should have received a copy of the GNU General Public License
 *  along with this program; if not, see <http://www.gnu.org/licenses/>.
 *
 */

import java.io.IOException;

import com.martiansoftware.jsap.FlaggedOption;
import com.martiansoftware.jsap.JSAP;
import com.martiansoftware.jsap.JSAPException;
import com.martiansoftware.jsap.JSAPResult;
import com.martiansoftware.jsap.Parameter;
import com.martiansoftware.jsap.SimpleJSAP;
import com.martiansoftware.jsap.UnflaggedOption;

import it.unimi.dsi.law.stat.CorrelationIndex;
import it.unimi.dsi.law.stat.KendallTau;
import it.unimi.dsi.law.stat.WeightedTau;
import it.unimi.dsi.logging.ProgressLogger;
import it.unimi.dsi.webgraph.ImmutableGraph;
import it.unimi.dsi.webgraph.LazyIntIterator;
import it.unimi.dsi.webgraph.NodeIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// RELEASE-STATUS: DIST

/** Computes Kendall's assortativities between the list of degrees of sources and targets of
 * arcs of a graph. */

public class KendallAssortativityOnline {

	protected KendallAssortativityOnline() {}

	/** Returns the assortativities of the specified graph with respect to the provided {@linkplain CorrelationIndex correlation index}.
	 *
	 * @param graph the graph.
	 * @param correlationIndex a correlation index.
	 * @return Assortativities of <code>graph</code> in the following order: (+/+, -/+, -/-, +/-, -+/-+)
	 * with respect to {@code correlationIndex}
	 */
	public static double[] compute(final ImmutableGraph graph, final CorrelationIndex correlationIndex) {
		final int[] indegree = new int[graph.numNodes()];
		final int[] outdegree = new int[graph.numNodes()];
		final double[] result = new double[5];

		// Compute indegrees and outdegrees.

		NodeIterator nodeIterator = graph.nodeIterator();
		for(int i = graph.numNodes(); i-- != 0 ;) {
			final int x = nodeIterator.nextInt();
			outdegree[x] = nodeIterator.outdegree();
			final LazyIntIterator successors = nodeIterator.successors();
			int s;
			while((s = successors.nextInt()) != -1) indegree[s]++;
		}

		final double[] left = new double[(int)graph.numArcs()];
		final double[] right = new double[(int)graph.numArcs()];

		// Compute assortativities

		nodeIterator = graph.nodeIterator();
		for(int i = graph.numNodes(), a = 0; i-- != 0 ;) {
			final int x = nodeIterator.nextInt();
			final LazyIntIterator successors = nodeIterator.successors();
			int s;
			while((s = successors.nextInt()) != -1) {
				left[a] = outdegree[x];
				right[a] = outdegree[s];
				a++;
			}
		}

		result[0] = correlationIndex.compute(left, right);

		nodeIterator = graph.nodeIterator();
		for(int i = graph.numNodes(), a = 0; i-- != 0 ;) {
			final int x = nodeIterator.nextInt();
			for(int s = nodeIterator.outdegree(); s-- != 0;) left[a++] = indegree[x];
		}

		result[1] = correlationIndex.compute(left, right);

		nodeIterator = graph.nodeIterator();
		for(int i = graph.numNodes(), a = 0; i-- != 0 ;) {
			nodeIterator.nextInt();
			final LazyIntIterator successors = nodeIterator.successors();
			int s;
			while((s = successors.nextInt()) != -1) right[a++] = indegree[s];
		}

		result[2] = correlationIndex.compute(left, right);

		nodeIterator = graph.nodeIterator();
		for(int i = graph.numNodes(), a = 0; i-- != 0 ;) {
			final int x = nodeIterator.nextInt();
			for(int s = nodeIterator.outdegree(); s-- != 0;) left[a++] = outdegree[x];
		}

		result[3] = correlationIndex.compute(left, right);

		nodeIterator = graph.nodeIterator();
		for(int i = graph.numNodes(), a = 0; i-- != 0 ;) {
			final int x = nodeIterator.nextInt();
			final LazyIntIterator successors = nodeIterator.successors();
			int s;
			while((s = successors.nextInt()) != -1) {
				left[a] += indegree[x];
				right[a] += outdegree[s];
				a++;
			}
		}

		result[4] = correlationIndex.compute(left, right);

		return result;
	}

	public static void main(final String[] arg) throws NumberFormatException, IOException, JSAPException {
		final SimpleJSAP jsap = new SimpleJSAP(KendallAssortativityOnline.class.getName(),
				"Prints Kendall's assortativities of a graph.",
				new Parameter[] {
						new FlaggedOption("index", JSAP.STRING_PARSER,  "t", JSAP.NOT_REQUIRED, 'i', "index", "The type of correlation index to be used (t=Kendall's tau, h=Hyperbolic weighted tau)"),
						new UnflaggedOption("basename", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, JSAP.NOT_GREEDY, "The basename of a graph."),
				}
		);

		final JSAPResult jsapResult = jsap.parse(arg);
		if (jsap.messagePrinted()) System.exit(1);

		final CorrelationIndex correlation;
		switch(jsapResult.getString("index")) {
			case "t":
				correlation = KendallTau.INSTANCE;
				break;
			case "h":
				correlation = WeightedTau.HYPERBOLIC;
				break;
			default:
				correlation = null;
				System.err.println("Unrecognized index " + jsapResult.getString("index"));
				System.exit(1);
		}

		final ImmutableGraph graph = ImmutableGraph.load(jsapResult.getString("basename"));
		final double[] result = compute(graph, correlation);
		System.out.println("+/+: " + result[0]);
		System.out.println("-/+: " + result[1]);
		System.out.println("-/-: " + result[2]);
		System.out.println("+/-: " + result[3]);
		System.out.println("-+/-+: " + result[4]);

	}
}
