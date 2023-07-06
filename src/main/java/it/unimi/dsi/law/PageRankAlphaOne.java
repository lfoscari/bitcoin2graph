package it.unimi.dsi.law;

import it.unimi.dsi.law.rank.PageRankParallelPowerSeries;
import it.unimi.dsi.law.util.Norm;
import it.unimi.dsi.webgraph.ImmutableGraph;
import org.slf4j.Logger;

public class PageRankAlphaOne extends PageRankParallelPowerSeries {
	public PageRankAlphaOne(ImmutableGraph transpose, int requestedThreads, Logger logger) {
		super(transpose, requestedThreads, logger);
	}

	@Override
	public double normDelta() {
		return Norm.L_1.compute(this.rank, this.previousRank);
	}
}
