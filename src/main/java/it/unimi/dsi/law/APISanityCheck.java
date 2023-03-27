package it.unimi.dsi.law;

import it.unimi.dsi.fastutil.io.BinIO;
import it.unimi.dsi.io.FileLinesByteArrayIterable;
import it.unimi.dsi.io.FileLinesByteArrayIterable.FileLinesIterator;
import it.unimi.dsi.logging.ProgressLogger;
import it.unimi.dsi.sux4j.mph.GOV3Function;
import it.unimi.dsi.webgraph.ImmutableGraph;

import org.apache.commons.lang3.ArrayUtils;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.BitSet;

import static it.unimi.dsi.law.Parameters.*;

public class APISanityCheck {
	/** Check the correctness of the graph by checking with the output
	 * of the Blockchair API <a href="https://blockchair.com/api/docs#link_200">Blockchair API</a>. */

	private static final boolean DEBUG = true;
	private static final Path TRANSACTIONSDIR = resources.resolve("baseline-transactions/stripped-transactions");
	private static final ProgressLogger pl = new ProgressLogger(LoggerFactory.getLogger(APISanityCheck.class), "transactions");
	private static int inconsitencies = 0;

	public static void main(String[] args) throws IOException, ClassNotFoundException {
		final ImmutableGraph graph = ImmutableGraph.loadMapped(basename.toString(), pl);
		if (!graph.randomAccess()) throw new IllegalArgumentException("Provided graph does not permit random access");

		pl.logger.info("Loading address map...");
		final GOV3Function<byte[]> addressMap = (GOV3Function<byte[]>) BinIO.loadObject(addressesMapFile.toFile());
		// pl.logger.info("Loading transaction map");
		// final GOV3Function<byte[]> transactionMap = (GOV3Function<byte[]>) BinIO.loadObject(transactionsMapFile.toFile());

		final File[] transactions = TRANSACTIONSDIR.toFile().listFiles((d, s) -> s.endsWith(".txt"));
		if (transactions == null) throw new NoSuchFileException("No transactions in " + TRANSACTIONSDIR);

		pl.start("Checking transactions...");

		for (File transaction: transactions) {
			final FileLinesIterator lines = new FileLinesByteArrayIterable(transaction.toString()).iterator();
			final byte[] inputs = lines.next();
			final byte[] outputs = lines.next();

			final String transactionHash = transaction.getName().substring(0, transaction.getName().length() - 4);
			// final long transactionId = transactionMap.getLong(transactionHash.getBytes());

			final BitSet inputDelim = ArrayUtils.indexesOf(inputs, (byte) 32);
			inputDelim.set(inputs.length);
			final BitSet outputDelim = ArrayUtils.indexesOf(outputs, (byte) 32);
			outputDelim.set(outputs.length);

			if (DEBUG) {
				System.out.println("\nTransaction file: " + transaction);
				System.out.println("Transaction: " + transactionHash); // + " (id: " + transactionId + ")");
				System.out.println("Inputs: " + new String(inputs) + " [" + inputDelim + "]");
				System.out.println("Outputs: " + new String(outputs) + " [" + outputDelim + "]");
			}

			int inputOffset = 0;
			for (final int inputSpace: inputDelim.stream().toArray()) {
				final byte[] input = Arrays.copyOfRange(inputs, inputOffset, inputSpace);
				final int inputId = (int) addressMap.getLong(input);

				if (inputId == addressMap.defaultReturnValue()) {
					pl.logger.error("Unknown input address " + new String(input) + " in transaction " + transactionHash);
					continue;
				}

				inputOffset = inputSpace + 1;

				final int[] successors = graph.successorArray(inputId);
				final int outdegree = graph.outdegree(inputId);

				int outputOffset = 0;
				for (final int outputSpace: outputDelim.stream().toArray()) {
					final byte[] output = Arrays.copyOfRange(outputs, outputOffset, outputSpace);
					final int outputId = (int) addressMap.getLong(output);

					if (outputId == addressMap.defaultReturnValue()) {
						pl.logger.error("Unknown output address " + new String(output) + " in transaction " + transactionHash);
						continue;
					}

					outputOffset = outputSpace + 1;
					if (DEBUG) System.out.println(new String(input) + " (id: " + inputId + ") -> " + new String(output) + " (id: " + outputId + ")");

					if (!contains(successors, 0, outdegree, outputId)) {
						pl.logger.error("Inconsistency for transaction " + transactionHash + " on input " + input + " and output " + output);
						inconsitencies++;
					}

					pl.lightUpdate();
				}

				if (DEBUG) System.out.println();
			}
		}

		pl.logger.info("Total inconsistencies: " + inconsitencies);
		pl.done();
	}

	private static boolean contains(int[] array, int from, int to, int key) {
		if (from > to) throw new IllegalArgumentException("from " + from + " > to " + to);
		for (int i = from; i < to; i++) if (array[i] == key) return true;
		return false;
	}
}
