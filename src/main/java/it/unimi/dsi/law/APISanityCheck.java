package it.unimi.dsi.law;

import it.unimi.dsi.fastutil.io.BinIO;
import it.unimi.dsi.fastutil.objects.ObjectList;
import it.unimi.dsi.io.FileLinesByteArrayIterable;
import it.unimi.dsi.logging.ProgressLogger;
import it.unimi.dsi.sux4j.mph.GOV3Function;
import it.unimi.dsi.webgraph.EFGraph;

import org.apache.commons.lang3.ArrayUtils;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.BitSet;
import java.util.stream.IntStream;

import static it.unimi.dsi.law.Parameters.*;

public class APISanityCheck {
	/** Check the correctness of the graph by checking with the output
	 * of the Blockchair API <a href="https://blockchair.com/api/docs#link_200">Blockchair API</a>. */

	private static final boolean DEBUG = false;
	private static final Path BASELINEFILES = resources.resolve("baseline-transactions/stripped-transactions");
	private static final ProgressLogger pl = new ProgressLogger(LoggerFactory.getLogger(APISanityCheck.class), "transactions");

	public static void main(String[] args) throws IOException, ClassNotFoundException {
		
		pl.logger.info("Loading graph");
		final EFGraph graph = EFGraph.loadOffline(basename.toString(), pl);
		if (!graph.randomAccess()) throw new IllegalArgumentException("Provided graph does not permit random access");

		pl.logger.info("Loading address map");
		final GOV3Function<byte[]> addressMap = (GOV3Function<byte[]>) BinIO.loadObject(addressesMapFile.toFile());
		// pl.logger.info("Loading transaction map");
		// final GOV3Function<byte[]> transactionMap = (GOV3Function<byte[]>) BinIO.loadObject(transactionsMapFile.toFile());

		final File[] transactions = BASELINEFILES.toFile().listFiles((d, s) -> s.endsWith(".txt"));
		if (transactions == null) throw new NoSuchFileException("Not transactions in " + BASELINEFILES);

		pl.start("Checking transactions");

		for (File transaction: transactions) {
			final FileLinesByteArrayIterable it = new FileLinesByteArrayIterable(transaction.toString());
			final ObjectList<byte[]> lines = it.allLines();
			final byte[] inputs = lines.get(0), outputs = lines.get(1);

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
				final long inputId = addressMap.getLong(input);
				inputOffset = inputSpace + 1;

				System.out.println(new String(input) + " " + inputId);

				if (inputId == addressMap.defaultReturnValue()) {
					pl.logger.error("Unknown input address " + new String(input) + " in transaction " + transactionHash);
					continue;
				}

				if (DEBUG) System.out.print(new String(input) + " -> ");

				int outputOffset = 0;
				for (final int outputSpace: outputDelim.stream().toArray()) {
					final byte[] output = Arrays.copyOfRange(outputs, outputOffset, outputSpace);
					final long outputId = addressMap.getLong(output);
					outputOffset = outputSpace + 1;

					if (outputId == addressMap.defaultReturnValue()) {
						pl.logger.error("Unknown output address " + new String(output) + " in transaction " + transactionHash);
						break;
					}

					if (DEBUG) System.out.print(new String(output) + ", ");

					final int[] successors = graph.successorArray((int) inputId); // remove elements after outdegree?
					if (IntStream.of(successors).noneMatch(s -> s == outputId)) {
						pl.logger.error("Inconsistency for transaction " + transactionHash + " on input " + input + " and output " + output);
					}

					pl.lightUpdate();
				}

				if (DEBUG) System.out.println();
			}
		}

		pl.done();
	}
}
