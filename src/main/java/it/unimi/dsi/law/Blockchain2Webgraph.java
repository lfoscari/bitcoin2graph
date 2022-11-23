package it.unimi.dsi.law;

import it.unimi.dsi.fastutil.io.BinIO;
import it.unimi.dsi.fastutil.objects.Object2LongFunction;
import it.unimi.dsi.logging.ProgressLogger;
import it.unimi.dsi.webgraph.BVGraph;
import it.unimi.dsi.webgraph.ScatteredArcsASCIIGraph;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.concurrent.LinkedBlockingQueue;

public class Blockchain2Webgraph implements Iterator<long[]>, Iterable<long[]> {
	private final LinkedBlockingQueue<long[]> arcs;
	private final Thread findMapping;

	public Blockchain2Webgraph (final LinkedBlockingQueue<long[]> arcs, final Thread findMapping) {
		this.arcs = arcs;
		this.findMapping = findMapping;

		this.findMapping.start();
	}

	public static void main (String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Logger logger = LoggerFactory.getLogger(Blockchain2Webgraph.class);
		ProgressLogger progress = new ProgressLogger(logger, Parameters.logInterval, Parameters.logTimeUnit, "arcs");
		progress.displayLocalSpeed = true;

		Parameters.graph.toFile().mkdir();

		LinkedBlockingQueue<long[]> arcs = new LinkedBlockingQueue<>();
		File addressLongFile = Parameters.addressLongMap.toFile();
		Object2LongFunction<String> addressLong = (Object2LongFunction<String>) BinIO.loadObject(addressLongFile);

		FindMapping fm = new FindMapping(arcs, addressLong, progress);

		File tempDir = Files.createTempDirectory(Parameters.resources, "bw_temp").toFile();
		tempDir.deleteOnExit();

		Thread t = new Thread(fm);
		Blockchain2Webgraph bw = new Blockchain2Webgraph(arcs, t);
		ScatteredArcsASCIIGraph graph = new ScatteredArcsASCIIGraph(bw.iterator(), false, false, 100_000, tempDir, progress);

		BVGraph.store(graph, Parameters.basename.toString());
		BinIO.storeObject(graph.ids, Parameters.ids.toFile());

		t.join();
	}

	@Override
	public Iterator<long[]> iterator () {
		return this;
	}

	@Override
	public boolean hasNext () {
		while (this.findMapping.isAlive()) {
			if (!this.arcs.isEmpty()) {
				return true;
			}
		}

		return !this.arcs.isEmpty();
	}

	@Override
	public long[] next () {
		try {
			return this.arcs.take();
		} catch (InterruptedException e) {
			throw new RuntimeException(e);
		}
	}
}
