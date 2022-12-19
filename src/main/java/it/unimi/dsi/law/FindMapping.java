package it.unimi.dsi.law;

import it.unimi.dsi.logging.ProgressLogger;
import org.rocksdb.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;

import static it.unimi.dsi.law.Parameters.*;
import static it.unimi.dsi.law.RocksDBWrapper.Column.INPUT;
import static it.unimi.dsi.law.RocksDBWrapper.Column.OUTPUT;

public class FindMapping implements Runnable {
	private final LinkedBlockingQueue<long[]> arcs;
	private final ProgressLogger progress;

	public FindMapping () {
		this(null, null);
	}

	public FindMapping (final LinkedBlockingQueue<long[]> arcs, ProgressLogger progress) {
		if (progress == null) {
			Logger logger = LoggerFactory.getLogger(this.getClass());
			progress = new ProgressLogger(logger, logInterval, logTimeUnit, "transaction");
		}

		this.arcs = arcs;
		this.progress = progress;
	}

	public void run () {
		try (RocksDBWrapper database = new RocksDBWrapper(true, transactionsDatabaseDirectory)) {
			this.progress.start("Searching mappings...");
			this.findMapping(database);
			this.progress.done();
		} catch (IOException | ClassNotFoundException | InterruptedException | RocksDBException e) {
			throw new RuntimeException(e);
		}
	}

	public void findMapping (RocksDBWrapper database) throws IOException, ClassNotFoundException, InterruptedException {
		try (RocksIterator inputIterator = database.iterator(INPUT);
			RocksIterator outputIterator = database.iterator(OUTPUT)) {

			ByteBuffer inputTransaction = ByteBuffer.allocate(20);
			ByteBuffer outputTransaction = ByteBuffer.allocate(20);

			inputIterator.seekToFirst();
			inputIterator.key(inputTransaction);

			for (outputIterator.seekToFirst(); outputIterator.isValid(); outputIterator.next()) {
				outputIterator.key(outputTransaction);

				for (; inputIterator.isValid(); inputIterator.next()) {
					inputIterator.key(inputTransaction);

					int cmp = Arrays.compareUnsigned(outputTransaction.array(), inputTransaction.array());

					if (cmp == 0) {
						this.addArcs(inputIterator.value(), outputIterator.value());
						break;
					} else if (cmp < 0) {
						break;
					}
				}
			}
		}
	}

	private void addArcs(byte[] inputsAddresses, byte[] outputsAddresses) throws InterruptedException {
		for (long inputAddress : Utils.bytesToLongs(inputsAddresses)) {
			for (long outputAddress : Utils.bytesToLongs(outputsAddresses)) {
				if (this.arcs != null) {
					this.arcs.put(new long[] { inputAddress, outputAddress });
				} else {
					System.out.println(inputAddress + " ~> " + outputAddress);
				}
			}
		}
	}

	public static void main (String[] args) throws RocksDBException {
		new FindMapping().run();
	}
}
