package it.unimi.dsi.law;

import com.martiansoftware.jsap.*;
import it.unimi.dsi.fastutil.floats.FloatArrays;
import it.unimi.dsi.fastutil.io.BinIO;
import it.unimi.dsi.logging.ProgressLogger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;

public class HarmonicDiameter {
	public static void main(String[] args) throws IOException, JSAPException {
		final SimpleJSAP jsap = new SimpleJSAP(HarmonicDiameter.class.getName(), "Compute the harmonic diameter.",
				new Parameter[]{
						new UnflaggedOption("harmonic", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, false, "The float-list file with the harmonic distances."),
				}
		);

		final JSAPResult jsapResult = jsap.parse(args);
		if (jsap.messagePrinted()) System.exit(1);

		final float[] harmonics = BinIO.loadFloats(jsapResult.getString("harmonic"));
		double sum = 0;
		for (double f: harmonics) sum += f;
		double diameter = harmonics.length * (harmonics.length - 1.0d) / sum;

		System.out.printf("%,f\n", diameter);
	}
}
