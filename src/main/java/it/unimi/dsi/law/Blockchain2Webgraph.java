package it.unimi.dsi.law;

import it.unimi.dsi.fastutil.io.BinIO;
import it.unimi.dsi.logging.ProgressLogger;
import it.unimi.dsi.webgraph.EFGraph;
import it.unimi.dsi.webgraph.ScatteredArcsASCIIGraph;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Queue;

import static it.unimi.dsi.law.Parameters.*;
import static it.unimi.dsi.law.RocksDBWrapper.Column.INPUT;
import static it.unimi.dsi.law.RocksDBWrapper.Column.OUTPUT;

public class Blockchain2Webgraph implements Iterator<long[]>, Iterable<long[]> {
	private final RocksDBWrapper database;

	private final ByteBuffer inputTransaction = ByteBuffer.allocate(Long.BYTES);
	private final ByteBuffer outputTransaction = ByteBuffer.allocate(Long.BYTES);

	private final RocksIterator inputIterator;
	private final RocksIterator outputIterator;

	private final Queue<long[]> arcs = new LinkedList<>();
	private final ProgressLogger progress;

	public Blockchain2Webgraph (ProgressLogger progress) throws RocksDBException {
		this.database = new RocksDBWrapper(true, transactionsDatabaseDirectory);

		this.inputIterator = this.database.iterator(INPUT);
		this.outputIterator = this.database.iterator(OUTPUT);

		this.inputIterator.seekToFirst();
		this.outputIterator.seekToFirst();

		this.progress = progress;
	}

	@Override
	public Iterator<long[]> iterator () {
		return this;
	}

	@Override
	public boolean hasNext () {
		if (!this.arcs.isEmpty()) {
			return true;
		}

		for (; this.outputIterator.isValid(); this.outputIterator.next()) {
			this.outputIterator.key(this.outputTransaction);

			for (; this.inputIterator.isValid(); this.inputIterator.next()) {
				this.inputIterator.key(this.inputTransaction);

				int cmp = Arrays.compareUnsigned(this.outputTransaction.array(), this.inputTransaction.array());
				if (cmp == 0) {
					this.addArcs(this.inputIterator.value(), this.outputIterator.value());
					this.outputIterator.next();
					return true;
				} else if (cmp < 0) {
					// Transaction is missing
					break;
				}
			}
		}

		this.close();
		return false;
	}

	@Override
	public long[] next () {
		this.progress.lightUpdate();
		return this.arcs.remove();
	}

	private void close () {
		this.inputIterator.close();
		this.outputIterator.close();
		this.database.close();
	}

	private void addArcs (byte[] inputsAddresses, byte[] outputsAddresses) {
		long[] outputAddressesLong = Utils.bytesToLongs(outputsAddresses);
		for (long inputAddress: Utils.bytesToLongs(inputsAddresses)) {
			for (long outputAddress: outputAddressesLong) {
				this.arcs.add(new long[] { inputAddress, outputAddress });
			}
		}
	}

	public static void main (String[] args) throws IOException, RocksDBException {
		Logger logger = LoggerFactory.getLogger(Blockchain2Webgraph.class);
		ProgressLogger progress = new ProgressLogger(logger, logInterval, logTimeUnit, "arcs");
		progress.displayLocalSpeed = true;

		graph.toFile().mkdir();

		Blockchain2Webgraph bw = new Blockchain2Webgraph(progress);
		File tempDir = Files.createTempDirectory(resources, "bw_temp").toFile();
		tempDir.deleteOnExit();

		ScatteredArcsASCIIGraph graph = new ScatteredArcsASCIIGraph(bw.iterator(), false, false, 10_000, tempDir, progress);

		EFGraph.store(graph, basename.toString());
		BinIO.storeObject(graph.ids, ids.toFile());
	}
}
