package it.unimi.dsi.law;

import com.martiansoftware.jsap.*;
import it.unimi.dsi.fastutil.io.FastBufferedInputStream;
import it.unimi.dsi.fastutil.longs.LongArrays;
import it.unimi.dsi.io.InputBitStream;
import it.unimi.dsi.logging.ProgressLogger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;

public class RawGraphStats {
	private static final Logger logger = LoggerFactory.getLogger(RawGraphStats.class);
	private static final ProgressLogger pl = new ProgressLogger(logger);

	public static void main(String[] args) throws IOException, JSAPException {
		final SimpleJSAP jsap = new SimpleJSAP(RawGraphStats.class.getName(), "Compute some stats on the given transaction graph",
				new Parameter[]{
						new UnflaggedOption("basename", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, false, "The basename of the graph."),
				}
		);

		final JSAPResult jsapResult = jsap.parse(args);
		if (jsap.messagePrinted()) System.exit(1);

		File labelFile = new File(jsapResult.getString("basename") + ".labels");
		InputBitStream fbis = new InputBitStream(Files.newInputStream(labelFile.toPath()));

		float labelSizeMean = 0f;
		float labelCount = 0f;

		long[] transactions = new long[512];
		int length;

		while (fbis.hasNext()) {
			length = fbis.readDelta();
			transactions = LongArrays.grow(transactions, length);
			int unique;

			if (length != 0) {
				for (int i = 0; i < length; i++) transactions[i] = fbis.readLongDelta();
				LongArrays.unstableSort(transactions);

				unique = 1;
				for (int i = 1; i < length; i++)
					if (transactions[i] == transactions[i - 1]) unique++;
			} else {
				unique = 0;
			}

			labelSizeMean = (labelSizeMean * labelCount) + unique / (labelCount + 1);
			labelCount++;
		}
	}
}
