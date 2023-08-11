package it.unimi.dsi.law;

import com.martiansoftware.jsap.*;
import it.unimi.dsi.fastutil.io.BinIO;

import java.io.IOException;

public class GiniCoefficient {
	public static void main(String[] args) throws IOException, JSAPException {
		final SimpleJSAP jsap = new SimpleJSAP(TransactionDegree.class.getName(), "Compute the Gini coefficient for the given double values.",
				new Parameter[]{
						new UnflaggedOption("values", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, false, "The values of which to compute the gini coefficient as doubles."),
				}
		);

		final JSAPResult jsapResult = jsap.parse(args);
		if (jsap.messagePrinted()) System.exit(1);

		final double[] values = BinIO.loadDoubles(jsapResult.getString("values"));
		System.out.println(gini(values));
	}

	static double gini(double[] values) {
		double sumOfDifference = 0;
		for (double a: values) for (double b: values)
			sumOfDifference += Math.abs(a - b);
		return sumOfDifference / (2 * values.length * values.length * mean(values));
	}

	static double mean(double[] values) {
		double sum = 0;
		for (double d: values) sum += d;
		return sum / values.length;
	}
}
