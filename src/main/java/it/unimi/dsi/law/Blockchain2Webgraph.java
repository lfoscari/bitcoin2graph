package it.unimi.dsi.law;

import it.unimi.dsi.fastutil.io.BinIO;
import it.unimi.dsi.fastutil.objects.Object2LongFunction;
import it.unimi.dsi.logging.ProgressLogger;
import it.unimi.dsi.webgraph.BVGraph;
import it.unimi.dsi.webgraph.EFGraph;
import it.unimi.dsi.webgraph.ScatteredArcsASCIIGraph;
import org.rocksdb.RocksDBException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Iterator;
import java.util.concurrent.LinkedBlockingQueue;

import static it.unimi.dsi.law.Parameters.*;

public class Blockchain2Webgraph implements Iterator<long[]>, Iterable<long[]> {
	private final LinkedBlockingQueue<long[]> arcs;
	private final Thread findMapping;

	public Blockchain2Webgraph (final LinkedBlockingQueue<long[]> arcs, final Thread findMapping) {
		this.arcs = arcs;
		this.findMapping = findMapping;
		this.findMapping.start();
	}

	public static void main (String[] args) throws IOException, InterruptedException, RocksDBException {
		Logger logger = LoggerFactory.getLogger(Blockchain2Webgraph.class);
		ProgressLogger progress = new ProgressLogger(logger, logInterval, logTimeUnit, "arcs");
		progress.displayLocalSpeed = true;

		graph.toFile().mkdir();

		LinkedBlockingQueue<long[]> arcs = new LinkedBlockingQueue<>();

		FindMapping fm = new FindMapping(arcs, progress);
		Thread t = new Thread(fm);
		Blockchain2Webgraph bw = new Blockchain2Webgraph(arcs, t);

		File tempDir = Files.createTempDirectory(resources, "bw_temp").toFile();
		tempDir.deleteOnExit();

		ScatteredArcsASCIIGraph graph = new ScatteredArcsASCIIGraph(bw.iterator(), false, false, 100_000, tempDir, progress);

		EFGraph.store(graph, basename.toString());
		BinIO.storeObject(graph.ids, ids.toFile());

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
