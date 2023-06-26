package it.unimi.dsi.law;

import com.martiansoftware.jsap.*;
import it.unimi.dsi.fastutil.doubles.DoubleArrays;
import it.unimi.dsi.fastutil.ints.IntArrays;
import it.unimi.dsi.fastutil.io.BinIO;
import it.unimi.dsi.fastutil.objects.Object2LongFunction;
import it.unimi.dsi.io.FileLinesMutableStringIterable;
import it.unimi.dsi.lang.MutableString;
import it.unimi.dsi.logging.ProgressLogger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class HeavyHitters {
	private static final Logger logger = LoggerFactory.getLogger(HeavyHitters.class);
	private static final ProgressLogger pl = new ProgressLogger(logger);

	public static void main(String[] args) throws JSAPException, IOException, ClassNotFoundException {
		final SimpleJSAP jsap = new SimpleJSAP(HeavyHitters.class.getName(), "Given a mapping from objects to nodes and a ranking on the nodes find the top objects according to the rank.",
				new Parameter[] {
						new FlaggedOption("amount", JSAP.INTEGER_PARSER, "100", JSAP.NOT_REQUIRED, 'a', "The number of heavyhitters to retrieve."),
						new FlaggedOption("ranking", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, 'r', "A ranking on the graph as doubles in binary form."),
						new FlaggedOption("objectMap", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, 'm', "The object map used to build the graph."),
						new FlaggedOption("objects", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, 'o', "A file with all the objects in string form."),
				}
		);

		final JSAPResult jsapResult = jsap.parse(args);
		if (jsap.messagePrinted()) System.exit(1);

		double[] rank = BinIO.loadDoubles(jsapResult.getString("ranking"));
		Object2LongFunction<byte[]> objectMap = (Object2LongFunction<byte[]>) BinIO.loadObject(jsapResult.getString("objectMap"));

		int amount = jsapResult.getInt("amount");

		// The quickselect find the k-th minimum value, we want the k-th maximum
		int k = rank.length - amount + 1;
		pl.start("Finding " + amount + "-th statistics");
		double max =  new Quickselect().quickselect(rank, k);
		pl.done();

		// Isolate the nodes with a rank above the threshold
		pl.start("Isolating heavy-hitting nodes");
		int[] nodes = new int[amount];
		int j = 0;
		for (int i = 0; i < rank.length; i++)
			if (rank[i] >= max) nodes[j++] = i;

		// Sort the nodes according to rank
		IntArrays.quickSort(nodes, (a, b) -> Double.compare(rank[a], rank[b]));
		pl.done();

		// Find the objects corresponding to the isolated nodes
		String[] hh = new String[amount];

		pl.start("Reverse-mapping nodes to objects");
		pl.expectedUpdates = amount;
		pl.itemsName = "nodes";
		for (MutableString obj: new FileLinesMutableStringIterable(jsapResult.getString("objects"))) {
			char[] cc = obj.toCharArray();
			byte[] bb = new byte[cc.length];
			for (int c = 0; c < cc.length; c++) bb[c] = (byte) cc[c];

			// Check if this obj corresponds to any of the heavyhitting nodes
			for (int i = 0; i < nodes.length; i++) {
				if (objectMap.getLong(bb) == nodes[i]) {
					hh[i] = obj.clone().toString();
					pl.update();
				}
			}
		}
		pl.done();

		for (int i = 0; i < nodes.length; i++)
			System.out.println(hh[i] + " (" + rank[nodes[i]] + ")");
	}

	private static class Quickselect {
		/** @credits <a href="https://github.com/JohnKurlak/Algorithms">source</a> */

		public Double quickselect(double[] list, int k) {
			return this.quickselect(list, 0, list.length - 1, k);
		}

		public Double quickselect(double[] list, int leftIndex, int rightIndex, int k) {
			// Edge case
			if (k < 1 || k > list.length)
				return null;

			// Base case
			if (leftIndex == rightIndex)
				return list[leftIndex];

			// Partition the sublist into two halves
			int pivotIndex = this.randomPartition(list, leftIndex, rightIndex);
			int sizeLeft = pivotIndex - leftIndex + 1;

			// Perform comparisons and recurse in binary search / quicksort fashion
			if (sizeLeft == k) {
				return list[pivotIndex];
			} else if (sizeLeft > k) {
				return this.quickselect(list, leftIndex, pivotIndex - 1, k);
			}

			return this.quickselect(list, pivotIndex + 1, rightIndex, k - sizeLeft);
		}

		public int randomPartition(double[] list, int leftIndex, int rightIndex) {
			int pivotIndex = this.medianOf3(list, leftIndex, rightIndex);
			double pivotValue = list[pivotIndex];
			int storeIndex = leftIndex;

			DoubleArrays.swap(list, pivotIndex, rightIndex);

			for (int i = leftIndex; i < rightIndex; i++) {
				if (list[i] <= pivotValue) {
					DoubleArrays.swap(list, storeIndex, i);
					storeIndex++;
				}
			}

			DoubleArrays.swap(list, rightIndex, storeIndex);
			return storeIndex;
		}

		public int medianOf3(double[] list, int leftIndex, int rightIndex) {
			int centerIndex = (leftIndex + rightIndex) / 2;

			if (list[leftIndex] > list[rightIndex])
				DoubleArrays.swap(list, leftIndex, centerIndex);

			if (list[leftIndex] > list[rightIndex])
				DoubleArrays.swap(list, leftIndex, rightIndex);

			if (list[centerIndex] > list[rightIndex])
				DoubleArrays.swap(list, centerIndex, rightIndex);

			DoubleArrays.swap(list, centerIndex, rightIndex - 1);
			return rightIndex - 1;
		}
	}
}
