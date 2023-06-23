package it.unimi.dsi.law;

import com.martiansoftware.jsap.JSAPException;
import com.martiansoftware.jsap.Parameter;
import com.martiansoftware.jsap.SimpleJSAP;
import com.martiansoftware.jsap.UnflaggedOption;
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
	private static final Logger logger = LoggerFactory.getLogger(RawGraphStats.class);
	private static final ProgressLogger pl = new ProgressLogger(logger);

	public static void main(String[] args) throws IOException, JSAPException, ClassNotFoundException {
		final SimpleJSAP jsap = new SimpleJSAP(MinerAddresses.class.getName(), "For each address count the number of mined blocks",
				new Parameter[]{
						new FlaggedOption("inputsDir", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, 'i', "The directory containing all the inputs in tar.gz."),
						new FlaggedOption("addressMapFile", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, 'a', "The file with the address map."),
						new FlaggedOption("addressMapFile", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, 'o', "The file to store the resulting integer array."),
				}
		);

		final JSAPResult jsapResult = jsap.parse(args);
		if (jsap.messagePrinted()) System.exit(1);

		File inputsDir = jsapResult.getFile("inputsDir");
		File[] inputs = inputsDir.listFiles((d, s) -> s.endsWith("tar.gz"));
		if (inputs == null || inputs.length == 0) throw new JSAPException("No inputs in " + inputsDir);

		File addressMapFile = jsapResult.getFile("addressMapFile");
		Object2LongFunction<byte[]> addressMap = (Object2LongFunction<byte[]>) BinIO.loadObject(addressMapFile);

		File outputFile = jsapResult.getFile("outputFile");

		pl.start("Loading inputs files");
		pl.expectedUpdates = inputs.length;

		int[] miners = new int[addressMap.size()];

		for (File input: inputs) {
			try (BufferedReader gzipReader = new BufferedReader(new InputStreamReader(new GZIPInputStream(Files.newInputStream(input.toPath()))))) {
				while (gzipReader.ready()) {
					MutableString line = new MutableString(gzipReader.readLine());

					// Check that the line is from coinbase
					if (Utils.column(line, 9).equals("0"))
						continue;

					byte[] address = Utils.columnBytes(line, 6);

					// Increment associated address values
					long addressId = addressMap.getLong(address);
					miners[(int) addressId]++;
				}
			}

			pl.update();
		}
		pl.done();

		BinIO.storeInts(miners, outputFile);
	}
}
