package it.unimi.dsi.law;

import com.martiansoftware.jsap.*;
import it.unimi.dsi.logging.ProgressLogger;
import it.unimi.dsi.webgraph.BVGraph;
import it.unimi.dsi.webgraph.ImmutableGraph;
import it.unimi.dsi.webgraph.algo.StronglyConnectedComponents;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class SCC {
	public static void main(String[] args) throws JSAPException, IOException {
		StronglyConnectedComponents.main(args);
	}
}
