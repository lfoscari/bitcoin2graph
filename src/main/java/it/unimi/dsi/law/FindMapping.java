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

			/* List<byte[]> outputs = new ArrayList<>();
			for (outputIterator.seekToFirst(); outputIterator.isValid(); outputIterator.next()) {
				outputs.add(outputIterator.key());
			}

			List<byte[]> inputs = new ArrayList<>();
			for (inputIterator.seekToFirst(); inputIterator.isValid(); inputIterator.next()) {
				inputs.add(inputIterator.key());
			}

			for (int i = 0; i < Integer.min(outputs.size(), inputs.size()); i++) {
				System.out.println(Arrays.toString(inputs.get(i)) + "\t\t\t" + Arrays.toString(outputs.get(i)));
			} */

			// Iterate before over the outputs because there are less
			inputIterator.seekToFirst();

			for (outputIterator.seekToFirst(); outputIterator.isValid(); outputIterator.next()) {
				// Can reuse a byte buffer in 'key'
				byte[] transaction = outputIterator.key();

				while (Arrays.compareUnsigned(transaction, inputIterator.key()) > 0) {
					inputIterator.next();
				}

				if (Arrays.equals(inputIterator.key(), transaction)) { // redundant
					this.addArcs(inputIterator.value(), outputIterator.value());
				}
			}

			/* outputIterator.seekToFirst();
			inputIterator.seekToFirst();

			ByteBuffer outputTransaction = ByteBuffer.allocate(Long.BYTES);
			ByteBuffer inputTransaction = ByteBuffer.allocate(Long.BYTES);

			for (; outputIterator.isValid(); outputIterator.next()) {
				outputIterator.key(outputTransaction);

				for (; inputIterator.isValid(); inputIterator.next()) {
					inputIterator.key(inputTransaction);
					int order = Arrays.compareUnsigned(outputTransaction.array(), inputTransaction.array());

					if (order > 0) {
						break;
					}

					if (order == 0) {
						this.addArcs(inputIterator.value(), outputIterator.value());
						break;
					}
				}
			} */
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
