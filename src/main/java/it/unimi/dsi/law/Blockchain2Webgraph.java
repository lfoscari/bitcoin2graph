package it.unimi.dsi.law;

import it.unimi.dsi.fastutil.io.BinIO;
import it.unimi.dsi.logging.ProgressLogger;
import it.unimi.dsi.sux4j.mph.GOV3Function;
import it.unimi.dsi.webgraph.BVGraph;
import it.unimi.dsi.webgraph.ScatteredArcsASCIIGraph;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.NoSuchElementException;
import java.util.Queue;

import static it.unimi.dsi.law.Parameters.*;

public class Blockchain2Webgraph implements Iterator<long[]>, Iterable<long[]> {
	private final TransactionsDatabase transactionsDatabase;
	private final GOV3Function<byte[]> transactionMap;
	private final Queue<long[]> arcs = new LinkedList<>();
	private final ProgressLogger progress;
	private long transaction = 0;

	public Blockchain2Webgraph(TransactionsDatabase transactionsDatabase, GOV3Function<byte[]> transactionMap, ProgressLogger progress) {
		this.transactionsDatabase = transactionsDatabase;
		this.transactionMap = transactionMap;
		this.progress = progress == null ? Utils.getProgressLogger(Blockchain2Webgraph.class, "arcs") : progress;
	}

	public static void main(String[] args) throws IOException {
		graphDir.toFile().mkdir();
		artifacts.toFile().mkdir();

		GOV3Function<byte[]> addressMap = MappingTables.buildAddressesMap();
		GOV3Function<byte[]> transactionMap = MappingTables.buildTransactionsMap();

		TransactionsDatabase transactions = new TransactionsDatabase(addressMap, transactionMap);

		ProgressLogger progress = Utils.getProgressLogger(Blockchain2Webgraph.class, "arcs");
		Blockchain2Webgraph bw = new Blockchain2Webgraph(transactions, transactionMap, progress);
		File tempDir = Files.createTempDirectory(resources, "bw_temp").toFile();
		tempDir.deleteOnExit();

		ScatteredArcsASCIIGraph graph = new ScatteredArcsASCIIGraph(bw.iterator(), false, false, batchSize, tempDir, progress);

		BVGraph.store(graph, basename.toString());
		BinIO.storeLongs(graph.ids, ids.toFile());
	}

	@Override
	public Iterator<long[]> iterator() {
		return this;
	}

	@Override
	public boolean hasNext() {
		if (!this.arcs.isEmpty()) {
			return true;
		}

		while (this.transaction < this.transactionMap.size64()) {
			long[] inputAddresses = this.transactionsDatabase.getInputAddresses(this.transaction);
			long[] outputAddresses = this.transactionsDatabase.getOutputAddresses(this.transaction);

			this.transaction++;

			if (inputAddresses.length == 0 || outputAddresses.length == 0) {
				continue;
			}

			for (long inputAddress : inputAddresses) {
				for (long outputAddress : outputAddresses) {
					this.arcs.add(new long[]{inputAddress, outputAddress});
				}
			}

			return true;
		}

		return false;
	}

	@Override
	public long[] next() {
		if (!this.hasNext()) {
			throw new NoSuchElementException();
		}

		this.progress.lightUpdate();
		return this.arcs.remove();
	}
}
