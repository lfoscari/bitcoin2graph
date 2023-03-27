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

	private static final boolean DEBUG = false;
	private static final Path TRANSACTIONSDIR = resources.resolve("baseline-transactions/stripped-transactions");
	private static final ProgressLogger pl = new ProgressLogger(LoggerFactory.getLogger(APISanityCheck.class), "transactions");
	private static int inconsitencies = 0;

	public static void main(String[] args) throws IOException, ClassNotFoundException {
		pl.logger.info("Loading address map...");
		final GOV3Function<byte[]> addressMap = (GOV3Function<byte[]>) BinIO.loadObject(addressesMapFile.toFile());
		pl.logger.info("Loading transaction map");
		final GOV3Function<byte[]> transactionMap = (GOV3Function<byte[]>) BinIO.loadObject(transactionsMapFile.toFile());
		pl.logger.info("Loading graph...");
		final ImmutableGraph graph = ImmutableGraph.loadMapped(basename.toString());
		if (!graph.randomAccess()) throw new IllegalArgumentException("Provided graph does not permit random access");

		final File[] transactions = TRANSACTIONSDIR.toFile().listFiles((d, s) -> s.endsWith(".txt"));
		if (transactions == null) throw new NoSuchFileException("No transactions in " + TRANSACTIONSDIR);

		pl.start("Checking transactions...");
		pl.itemsName = "transactions";

		for (File transaction: transactions) {
			final FileLinesIterator lines = new FileLinesByteArrayIterable(transaction.toString()).iterator();
			final byte[] inputs = lines.next();
			final byte[] outputs = lines.next();

			final String transactionHash = transaction.getName().substring(0, transaction.getName().length() - 4);
			final long transactionId = transactionMap.getLong(transactionHash.getBytes());

			if (transactionId == transactionMap.defaultReturnValue()) {
				pl.logger.error("Unknown transaction " + transactionHash);
				continue;
			}

			final BitSet inputDelimSet = ArrayUtils.indexesOf(inputs, (byte) 32);
			inputDelimSet.set(inputs.length);
			inputDelimSet.set(0);
			final BitSet outputDelimSet = ArrayUtils.indexesOf(outputs, (byte) 32);
			outputDelimSet.set(outputs.length);
			outputDelimSet.set(0);

			int[] inputOffsets = inputDelimSet.stream().toArray();
			int[] outputOffsets = outputDelimSet.stream().toArray();

			if (DEBUG) {
				System.out.println("\nTransaction file: " + transaction);
				System.out.println("Transaction: " + transactionHash + " (id: " + transactionId + ")");
				System.out.println("Inputs: " + new String(inputs) + " " + Arrays.toString(inputOffsets));
				System.out.println("Outputs: " + new String(outputs) + " " + Arrays.toString(outputOffsets));
			}

			for (int i = 1; i < inputOffsets.length; i++) {
				final byte[] input = Arrays.copyOfRange(inputs, inputOffsets[i - 1] + + (i == 1 ? 0 : 1), inputOffsets[i]);
				final int inputId = (int) addressMap.getLong(input);

				if (inputId == addressMap.defaultReturnValue()) {
					pl.logger.error("Unknown input address " + new String(input) + " in transaction " + transactionHash);
					continue;
				} 

				int[] successors;
				try {
					successors = graph.successorArray(inputId);
				} catch (ArrayIndexOutOfBoundsException e) {
					pl.logger.error(e.getClass().getSimpleName() + ": " + e.getMessage() + " (input id: " + inputId + ")");
					continue;
				}

				final int outdegree = graph.outdegree(inputId);

				for (int j = 1; j < outputOffsets.length; j++) {
					final byte[] output = Arrays.copyOfRange(outputs, outputOffsets[j - 1] + (j == 1 ? 0 : 1), outputOffsets[j]);
					final int outputId = (int) addressMap.getLong(output);

					pl.lightUpdate();

					if (outputId == inputId) {
						continue;
					}

					if (outputId == addressMap.defaultReturnValue()) {
						pl.logger.error("Unknown output address " + new String(output) + " in transaction " + transactionHash);
						continue;
					}

					if (DEBUG) System.out.println(new String(input) + " (id: " + inputId + ") -> " + new String(output) + " (id: " + outputId + ")");

					if (!contains(successors, 0, outdegree, outputId)) {
						pl.logger.error("Inconsistency for transaction " + transactionHash + " on input " + new String(input) + " and output " + new String(output));
						inconsitencies++;
					}
				}
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
