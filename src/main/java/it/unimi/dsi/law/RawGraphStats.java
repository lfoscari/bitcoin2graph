package it.unimi.dsi.law;

import com.martiansoftware.jsap.*;
import it.unimi.dsi.fastutil.io.FastBufferedInputStream;
import it.unimi.dsi.fastutil.longs.LongArrays;
import it.unimi.dsi.io.InputBitStream;
import it.unimi.dsi.logging.ProgressLogger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.EOFException;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.util.concurrent.TimeUnit;

public class RawGraphStats {
	private static final Logger logger = LoggerFactory.getLogger(RawGraphStats.class);
	private static final ProgressLogger pl = new ProgressLogger(logger);

	public static void main(String[] args) throws IOException, JSAPException {
		final SimpleJSAP jsap = new SimpleJSAP(RawGraphStats.class.getName(), "Compute some stats on the given transaction graph",
				new Parameter[]{
						new UnflaggedOption("basename", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, false, "The basename of the graph."),
						new UnflaggedOption("output", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, false, "The output file."),
				}
		);

		final JSAPResult jsapResult = jsap.parse(args);
		if (jsap.messagePrinted()) System.exit(1);

		File labelFile = new File(jsapResult.getString("basename") + ".labels");
		InputBitStream fbis = new InputBitStream(Files.newInputStream(labelFile.toPath()));

		pl.start("Computing: label size mean");
		pl.itemsName = "nodes";
		pl.logInterval = TimeUnit.SECONDS.toMillis(10);

		double labelSizeMean = 0f;
		double labelCount = 0f;

		long[] transactions = new long[512];
		int length;

		while (fbis.hasNext()) {
			try {
				length = fbis.readDelta();
			} catch (EOFException e) {
				break;
			}
			transactions = LongArrays.grow(transactions, length);
			int unique;

			if (length != 0) {
				for (int i = 0; i < length; i++) transactions[i] = fbis.readLongDelta();
				LongArrays.unstableSort(transactions, 0, length);

				unique = 1;
				for (int i = 1; i < length; i++)
					if (transactions[i] == transactions[i - 1]) unique++;
			} else {
				unique = 0;
			}

			labelSizeMean = ((labelSizeMean * labelCount) + unique) / (labelCount + 1);
			labelCount++;

			pl.lightUpdate();
		}

		System.out.println(labelSizeMean);

		File outputFile = new File(jsapResult.getString("output"));
		try (FileOutputStream fos = new FileOutputStream(outputFile)) {
			fos.write(Double.toString(labelSizeMean).getBytes());
		}

		pl.done();
	}
}
