package it.unimi.dsi.law;

import it.unimi.dsi.io.InputBitStream;
import it.unimi.dsi.io.OutputBitStream;
import it.unimi.dsi.webgraph.labelling.Label;

import java.io.File;
import java.io.IOException;
import java.util.List;

public class Copied {
	/** Sorts the given source and target arrays w.r.t. the target and stores them in two temporary files.
	 *  An additional positionable input bit stream is provided that contains labels, starting at given positions.
	 *  Labels are also written onto the appropriate file.
	 *
	 * @param n the index of the last element to be sorted (exclusive).
	 * @param source the source array.
	 * @param target the target array.
	 * @param start the array containing the bit position (within the given input stream) where the label of the arc starts.
	 * @param labelBitStream the positionable bit stream containing the labels.
	 * @param tempDir a temporary directory where to store the sorted arrays.
	 * @param batches a list of files to which the batch file will be added.
	 * @param labelBatches a list of files to which the label batch file will be added.
	 */

	protected static void processTransposeBatch(final int n, final int[] source, final int[] target, final long[] start,
											  final InputBitStream labelBitStream, final File tempDir, final List<File> batches, final List<File> labelBatches,
											  final Label prototype) throws IOException {
		it.unimi.dsi.fastutil.Arrays.parallelQuickSort(0, n, (x,y) -> {
					final int t = Integer.compare(source[x], source[y]);
					if (t != 0) return t;
					return Integer.compare(target[x], target[y]);
				},
				(x, y) -> {
					int t = source[x];
					source[x] = source[y];
					source[y] = t;
					t = target[x];
					target[x] = target[y];
					target[y] = t;
					final long u = start[x];
					start[x] = start[y];
					start[y] = u;
				});

		final File batchFile = File.createTempFile("batch", ".bitstream", tempDir);
		batchFile.deleteOnExit();
		batches.add(batchFile);
		final OutputBitStream batch = new OutputBitStream(batchFile);

		if (n != 0) {
			// Compute unique pairs
			batch.writeDelta(n);
			int prevSource = source[0];
			batch.writeDelta(prevSource);
			batch.writeDelta(target[0]);

			for(int i = 1; i < n; i++) {
				if (source[i] != prevSource) {
					batch.writeDelta(source[i] - prevSource);
					batch.writeDelta(target[i]);
					prevSource = source[i];
				}
				else if (target[i] != target[i - 1]) {
					// We don't write duplicate pairs
					batch.writeDelta(0);
					batch.writeDelta(target[i] - target[i - 1] - 1);
				}
			}
		}
		else batch.writeDelta(0);

		batch.close();

		final File labelFile = File.createTempFile("label-", ".bits", tempDir);
		labelFile.deleteOnExit();
		labelBatches.add(labelFile);
		final OutputBitStream labelObs = new OutputBitStream(labelFile);
		for (int i = 0; i < n; i++) {
			labelBitStream.position(start[i]);
			prototype.fromBitStream(labelBitStream, source[i]);
			prototype.toBitStream(labelObs, target[i]);
		}
		labelObs.close();
	}
}
