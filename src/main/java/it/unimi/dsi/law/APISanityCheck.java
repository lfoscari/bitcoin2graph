package it.unimi.dsi.law;

import it.unimi.dsi.fastutil.io.BinIO;
import it.unimi.dsi.fastutil.io.FastBufferedInputStream;
import it.unimi.dsi.fastutil.objects.ObjectList;
import it.unimi.dsi.io.FileLinesByteArrayIterable;
import it.unimi.dsi.io.FileLinesByteArrayIterable.FileLinesIterator;
import it.unimi.dsi.logging.ProgressLogger;
import it.unimi.dsi.sux4j.mph.GOV3Function;
import it.unimi.dsi.webgraph.ImmutableGraph;
import org.apache.commons.lang3.ArrayUtils;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.BitSet;
import java.util.List;
import java.util.stream.IntStream;

import static it.unimi.dsi.law.Parameters.*;

public class APISanityCheck {
	/** Check the correctness of the graph by checking with the output
	 * of the Blockchair API <a href="https://blockchair.com/api/docs#link_200">Blockchair API</a>.
	 */

	private static final Path transactionBaselineFiles = resources.resolve("baseline-transactions/stripped-transactions");

	public static void main(String[] args) throws IOException, ClassNotFoundException {
		final ProgressLogger pl = new ProgressLogger(LoggerFactory.getLogger(APISanityCheck.class), "transactions");
		pl.logger.info("Loading graph");
		final ImmutableGraph graph = ImmutableGraph.loadOffline(basename.toString());
		pl.logger.info("Loading address map");
		final GOV3Function<byte[]> addressMap = (GOV3Function<byte[]>) BinIO.loadObject(addressesMapFile.toFile());
		pl.logger.info("Loading transaction map");
		final GOV3Function<byte[]> transactionMap = (GOV3Function<byte[]>) BinIO.loadObject(transactionsMapFile.toFile());

		final File[] transactions = transactionBaselineFiles.toFile().listFiles((d, s) -> s.endsWith(".txt"));
		if (transactions == null) throw new NoSuchFileException("Not transactions in " + transactionBaselineFiles);

		pl.start("Checking transactions");

		for (File transaction: transactions) {
			FileLinesByteArrayIterable it = new FileLinesByteArrayIterable(transaction.toString());
			final ObjectList<byte[]> lines = it.allLines();
			byte[] inputs = lines.get(0), outputs = lines.get(1);

			final String transactionHash = transaction.getName().substring(0, transaction.getName().length() - 4);
			// final long transactionId = transactionMap.getLong(transactionHash.getBytes());

			int inputOffset = 0;
			BitSet inputDelim = ArrayUtils.indexesOf(inputs, (byte) 32);

			for (int inputSpace: inputDelim.stream().toArray()) {
				byte[] input = Arrays.copyOfRange(inputs, inputOffset, inputSpace);
				inputOffset += inputSpace;
				final long inputId = addressMap.getLong(input);

				int outputOffset = 0;
				BitSet outputDelim = ArrayUtils.indexesOf(outputs, (byte) 32);

				for (int outputSpace: outputDelim.stream().toArray()) {
					byte[] output = Arrays.copyOfRange(outputs, outputOffset, outputSpace);
					outputOffset += outputSpace;
					final long outputId = addressMap.getLong(output);

					final int[] successors = graph.successorArray((int) inputId);
					if (IntStream.of(successors).noneMatch(s -> s == outputId)) {
						pl.logger.error("Inconsistency for transaction " + transactionHash + " on input " + input + " and output " + output);
					}

					pl.lightUpdate();
				}
			}
		}

		pl.done();
	}
}
