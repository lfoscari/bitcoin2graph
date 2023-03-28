package it.unimi.dsi.law;

import it.unimi.dsi.fastutil.io.BinIO;
import it.unimi.dsi.io.FileLinesByteArrayIterable;
import it.unimi.dsi.io.FileLinesByteArrayIterable.FileLinesIterator;
import it.unimi.dsi.logging.ProgressLogger;
import it.unimi.dsi.sux4j.mph.GOV3Function;

import org.apache.commons.lang3.ArrayUtils;
import org.slf4j.LoggerFactory;
import it.unimi.dsi.webgraph.ImmutableGraph;

import java.io.File;
import java.io.IOException;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.BitSet;

import static it.unimi.dsi.law.Parameters.*;

public class APISanityCheck {
	/** Check the correctness of the graph by checking with the output
	 * of the BlockChair API <a href="https://blockchair.com/api/docs#link_200">Blockchair API</a>. */

	private static final boolean DEBUG = false;

	private static final Path transactionsDir = resources.resolve("baseline-transactions/stripped-transactions");
	private static final ProgressLogger pl = new ProgressLogger(LoggerFactory.getLogger(APISanityCheck.class), "transactions");
	private static int inconsistencies = 0;

	public static void main(String[] args) throws IOException, ClassNotFoundException {
		pl.logger.info("Loading address map...");
		final GOV3Function<byte[]> addressMap = (GOV3Function<byte[]>) BinIO.loadObject(addressesMapFile.toFile());
		pl.logger.info("Loading transaction map");
		final GOV3Function<byte[]> transactionMap = (GOV3Function<byte[]>) BinIO.loadObject(transactionsMapFile.toFile());
		pl.logger.info("Loading graph...");
		final ImmutableGraph graph = ImmutableGraph.loadMapped(basename.toString());
		if (!graph.randomAccess()) throw new IllegalArgumentException("Provided graph does not permit random access");

		final File[] transactions = transactionsDir.toFile().listFiles((d, s) -> s.endsWith(".txt"));
		if (transactions == null) throw new NoSuchFileException("No transactions in " + transactionsDir);

		pl.start("Checking transactions...");
		pl.itemsName = "transactions";
		pl.expectedUpdates = transactions.length;

		for (File transaction: transactions) {
			final byte[] inputs;
			final byte[] outputs;
			try (FileLinesIterator lines = new FileLinesByteArrayIterable(transaction.toString()).iterator()) {
				inputs = lines.next();
				outputs = lines.next();
			}

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

			final int[] inputOffsets = inputDelimSet.stream().toArray();
			final int[] outputOffsets = outputDelimSet.stream().toArray();

			if (DEBUG) {
				System.out.println("Transaction file: " + transaction);
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

				final int[] successors = graph.successorArray(inputId);
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

					if (!contains(successors, outdegree, outputId)) {
						pl.logger.error("Inconsistency for transaction " + transactionHash + " on input " + new String(input) + " and output " + new String(output));
						inconsistencies++;
					}
				}
			}

			pl.lightUpdate();
		}

		pl.logger.info("Total inconsistencies: " + inconsistencies);
		pl.done();
	}

	private static boolean contains(int[] array, int length, int key) {
		for (int i = 0; i < length; i++) if (array[i] == key) return true;
		return false;
	}
}
