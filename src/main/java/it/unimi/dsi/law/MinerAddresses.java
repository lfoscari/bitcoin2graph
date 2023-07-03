package it.unimi.dsi.law;

import com.martiansoftware.jsap.JSAPException;
import com.martiansoftware.jsap.Parameter;
import com.martiansoftware.jsap.SimpleJSAP;
import com.martiansoftware.jsap.UnflaggedOption;
import it.unimi.dsi.fastutil.doubles.DoubleList;
import it.unimi.dsi.fastutil.ints.IntArrays;
import it.unimi.dsi.fastutil.io.BinIO;
import it.unimi.dsi.fastutil.objects.Object2LongFunction;
import it.unimi.dsi.lang.MutableString;
import it.unimi.dsi.logging.ProgressLogger;


import com.martiansoftware.jsap.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.file.Files;
import java.util.zip.GZIPInputStream;

public class MinerAddresses {
	private static final Logger logger = LoggerFactory.getLogger(MinerAddresses.class);
	private static final ProgressLogger pl = new ProgressLogger(logger);

	public static void main(String[] args) throws IOException, JSAPException, ClassNotFoundException {
		final SimpleJSAP jsap = new SimpleJSAP(MinerAddresses.class.getName(), "For each address count the number of mined blocks",
				new Parameter[]{
						new FlaggedOption("outputsDir", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, 'i', "The directory containing all the outputs in gz."),
						new FlaggedOption("addressMapFile", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, 'a', "The file with the address map."),
						new FlaggedOption("outputFile", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, 'o', "The file to store the resulting integer array."),
				}
		);

		final JSAPResult jsapResult = jsap.parse(args);
		if (jsap.messagePrinted()) System.exit(1);

		File outputsDir = new File(jsapResult.getString("outputsDir"));
		if (!outputsDir.exists() || !outputsDir.isDirectory()) throw new JSAPException(outputsDir + " either does not exist or is not a directory");
		File[] outputs = outputsDir.listFiles((d, s) -> s.endsWith("tsv.gz"));
		if (outputs == null || outputs.length == 0) throw new JSAPException("No inputs in " + outputsDir);

		File addressMapFile = new File(jsapResult.getString("addressMapFile"));
		if (!addressMapFile.exists()) throw new JSAPException(addressMapFile + " does not exist");
		Object2LongFunction<byte[]> addressMap = (Object2LongFunction<byte[]>) BinIO.loadObject(addressMapFile);

		File outputFile = new File(jsapResult.getString("outputFile"));

		pl.start("Loading inputs files");
		pl.expectedUpdates = outputs.length;
		pl.itemsName = "files";

		int[] miners = new int[addressMap.size()];
		int unknown = 0;

		for (File input: outputs) {
			try (BufferedReader gzipReader = new BufferedReader(new InputStreamReader(new GZIPInputStream(Files.newInputStream(input.toPath()))))) {
				while (gzipReader.ready()) {
					MutableString line = new MutableString(gzipReader.readLine());

					// Check that the line is from coinbase
					if (!Utils.column(line, 9).equals("1"))
						continue;

					byte[] address = Utils.columnBytes(line, 6);

					// Increment associated address values
					long addressId = addressMap.getLong(address);

					if (addressId == -1) unknown++;
					else miners[(int) addressId]++;
				}
			}

			if (pl.count % 100 == 0)
				pl.logger.info("Unknown addresses: " + unknown);
			pl.update();
		}
		pl.done();

		pl.logger.info("Unknown addresses: " + unknown);

		int sum = 0;
		for (int m: miners) sum += m;

		double[] miners_p = new double[miners.length];
		for (int i = 0; i < miners.length; i++)
			miners_p[i] = (double) miners[i] / sum;

		BinIO.storeDoubles(miners_p, outputFile);
	}
}
