package it.unimi.dsi.law;

import com.martiansoftware.jsap.*;
import it.unimi.dsi.fastutil.doubles.DoubleArrays;
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
		DoubleArrays.parallelRadixSort(values);
		System.out.println(gini(values));
	}

	static double gini(double[] values) {
		double n = values.length;
		double sum = 0, sum_i = 0;
		for (int i = 0; i < n; i++) {
			sum += values[i];
			sum_i += i * values[i];
		}
		return (2 / n) * (sum_i / sum) - ((n + 1) / n);
	}
}
