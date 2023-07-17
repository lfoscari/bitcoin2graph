package it.unimi.dsi.law;

import com.martiansoftware.jsap.*;
import it.unimi.dsi.fastutil.Arrays;
import it.unimi.dsi.fastutil.doubles.DoubleArrays;
import it.unimi.dsi.fastutil.ints.IntArrays;
import it.unimi.dsi.fastutil.io.BinIO;
import it.unimi.dsi.fastutil.io.FastBufferedOutputStream;
import it.unimi.dsi.fastutil.objects.Object2LongFunction;
import it.unimi.dsi.fastutil.objects.ObjectArrays;
import it.unimi.dsi.io.FileLinesByteArrayIterable;
import it.unimi.dsi.io.FileLinesMutableStringIterable;
import it.unimi.dsi.lang.MutableString;
import it.unimi.dsi.logging.ProgressLogger;
import org.apache.commons.lang3.ArrayUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

import static it.unimi.dsi.io.FileLinesMutableStringIterable.*;
import static org.apache.commons.lang3.ArrayUtils.INDEX_NOT_FOUND;

public class HeavyHitters {
	private static final Logger logger = LoggerFactory.getLogger(HeavyHitters.class);
	private static final ProgressLogger pl = new ProgressLogger(logger);

	public static void main(String[] args) throws JSAPException, IOException, ClassNotFoundException {
		final SimpleJSAP jsap = new SimpleJSAP(HeavyHitters.class.getName(), "Given a mapping from addresses to nodes and a ranking on the nodes find the top addresses according to the rank.",
				new Parameter[] {
						new FlaggedOption("amount", JSAP.INTEGER_PARSER, "100", JSAP.NOT_REQUIRED, 'a', "The number of heavy-hitters to retrieve."),
						new FlaggedOption("ranking", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, 'r', "A ranking on the graph as doubles in binary form."),
						new FlaggedOption("addressMap", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, 'm', "The address map used to build the graph."),
						new FlaggedOption("addresses", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, 'o', "A file with all the addresses in string form."),
						new UnflaggedOption("outputFile", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.NOT_REQUIRED, false, "File where the heavy-hitters will be written, otherwise stdout.")
				}
		);

		final JSAPResult jsapResult = jsap.parse(args);
		if (jsap.messagePrinted()) System.exit(1);

		double[] rank = BinIO.loadDoubles(jsapResult.getString("ranking"));
		final Object2LongFunction<byte[]> addressMap = (Object2LongFunction<byte[]>) BinIO.loadObject(jsapResult.getString("addressMap"));

		final int amount = jsapResult.getInt("amount");

		// The quickselect find the k-th minimum value, we want the k-th maximum
		final int k = rank.length - amount + 1;
		pl.start("Finding " + amount + "-th statistics");
		final double max = new Quickselect().quickselect(rank, k);
		pl.done();

		// Isolate the nodes with a rank above the threshold
		pl.start("Isolating heavy-hitting nodes");
		final int[] nodes = new int[amount];
		int j = 0;
		for (int i = 0; i < rank.length && j < amount; i++)
			if (rank[i] >= max) nodes[j++] = i;

		// Sort the nodes according to rank
		// IntArrays.quickSort(nodes, (a, b) -> Double.compare(rank[a], rank[b]));
		pl.done();

		// Find the addresses corresponding to the isolated nodes
		// Considering that the addressMap is made starting from the addresses themselves,
		// we can simply find the address associated with node v by checking the v-th row.

		pl.start("Reverse-mapping nodes to addresses");
		pl.expectedUpdates = addressMap.size();
		pl.itemsName = "nodes";

		final String[] hh = new String[amount];
		MutableString address;
		int current = 0;

		try (FileLinesIterator it = new FileLinesMutableStringIterable(jsapResult.getString("addresses")).iterator()) {
			for (int addressId = 0; addressId < addressMap.size(); addressId++) {
				address = it.next();
				pl.lightUpdate();
				if (addressId != nodes[current]) continue;

				hh[current++] = address.toString();
			}
		}

		pl.done();

		int[] perm = new int[amount];
		for (int i = 0; i < perm.length; i++) perm[i] = i;

		// We are only interested in the rank of the heavyhitters, we can forget the rest
		double[] nodesrank = new double[amount];
		for (int i = 0; i < nodesrank.length; i++) nodesrank[i] = rank[nodes[i]];
		rank = nodesrank;

		DoubleArrays.quickSortIndirect(perm, nodesrank);
		for (int i = 0; i < perm.length; i++) ObjectArrays.swap(hh, i, perm[i]);

		if (jsapResult.contains("outputFile")) {
			try (final FastBufferedOutputStream fbos = new FastBufferedOutputStream(Files.newOutputStream(Paths.get(jsapResult.getString("outputFile"))))) {
				for (int i = nodes.length - 1; i >= 0; i--)
					fbos.write((hh[i] + " (" + rank[nodes[i]] + ")\n").getBytes());
			}
		} else {
			for (int i = nodes.length - 1; i >= 0; i--)
				System.out.println(hh[i] + " (" + rank[nodes[i]] + ")");
		}
	}

	private static class Quickselect {
		/** @credits <a href="https://github.com/JohnKurlak/Algorithms">John Kurlak</a> */

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
			int centerIndex = (int) (((long) leftIndex + (long) rightIndex) / 2);

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
