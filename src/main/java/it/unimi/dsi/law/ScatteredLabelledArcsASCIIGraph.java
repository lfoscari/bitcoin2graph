/*
 * Copyright (C) 2011-2023 Sebastiano Vigna
 *
 * This program and the accompanying materials are made available under the
 * terms of the GNU Lesser General Public License v2.1 or later,
 * which is available at
 * http://www.gnu.org/licenses/old-licenses/lgpl-2.1-standalone.html,
 * or the Apache Software License 2.0, which is available at
 * https://www.apache.org/licenses/LICENSE-2.0.
 *
 * This program is distributed in the hope that it will be useful, but
 * WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY
 * or FITNESS FOR A PARTICULAR PURPOSE.
 *
 * SPDX-License-Identifier: LGPL-2.1-or-later OR Apache-2.0
 */

package it.unimi.dsi.law;

import com.martiansoftware.jsap.*;
import it.unimi.dsi.fastutil.BigArrays;
import it.unimi.dsi.fastutil.Hash;
import it.unimi.dsi.fastutil.booleans.BooleanBigArrays;
import it.unimi.dsi.fastutil.bytes.ByteArrays;
import it.unimi.dsi.fastutil.ints.IntBigArrays;
import it.unimi.dsi.fastutil.io.BinIO;
import it.unimi.dsi.fastutil.io.FastBufferedInputStream;
import it.unimi.dsi.fastutil.longs.LongBigArrays;
import it.unimi.dsi.fastutil.objects.Object2LongFunction;
import it.unimi.dsi.fastutil.objects.Object2ObjectArrayMap;
import it.unimi.dsi.fastutil.objects.Object2ObjectFunction;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import it.unimi.dsi.logging.ProgressLogger;
import it.unimi.dsi.webgraph.BVGraph;
import it.unimi.dsi.webgraph.ImmutableSequentialGraph;
import it.unimi.dsi.webgraph.NodeIterator;
import it.unimi.dsi.webgraph.Transform;
import it.unimi.dsi.webgraph.labelling.Label;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.concurrent.TimeUnit;
import java.util.zip.GZIPInputStream;

import static it.unimi.dsi.fastutil.HashCommon.bigArraySize;
import static it.unimi.dsi.fastutil.HashCommon.maxFill;


/**
 * Da riscrivere
 */

public class ScatteredLabelledArcsASCIIGraph extends ImmutableSequentialGraph {
	private static final Logger LOGGER = LoggerFactory.getLogger(ScatteredLabelledArcsASCIIGraph.class);
	private final static boolean DEBUG = false;

	/**
	 * The default batch size.
	 */
	public static final int DEFAULT_BATCH_SIZE = 1000000;
	/**
	 * The extension of the identifier file (a binary list of longs).
	 */
	private static final String IDS_EXTENSION = ".ids";
	/**
	 * The batch graph used to return node iterators.
	 */
	private final Transform.BatchGraph batchGraph;
	/**
	 * The list of identifiers in order of appearance.
	 */
	public long[] ids;

	private static final class Long2IntOpenHashBigMap implements java.io.Serializable, Cloneable, Hash {
		public static final long serialVersionUID = 0L;

		/**
		 * The big array of keys.
		 */
		public transient long[][] key;

		/**
		 * The big array of values.
		 */
		public transient int[][] value;

		/**
		 * The big array telling whether a position is used.
		 */
		private transient boolean[][] used;

		/**
		 * The acceptable load factor.
		 */
		private final float f;

		/**
		 * The current table size (always a power of 2).
		 */
		private transient long n;

		/**
		 * Threshold after which we rehash. It must be the table size times {@link #f}.
		 */
		private transient long maxFill;

		/**
		 * The mask for wrapping a position counter.
		 */
		private transient long mask;

		/**
		 * The mask for wrapping a segment counter.
		 */
		private transient int segmentMask;

		/**
		 * The mask for wrapping a base counter.
		 */
		private transient int baseMask;

		/**
		 * Number of entries in the set.
		 */
		private long size;

		/**
		 * Initialises the mask values.
		 */
		private void initMasks() {
			this.mask = this.n - 1;
			/*
			 * Note that either we have more than one segment, and in this case all segments are
			 * BigArrays.SEGMENT_SIZE long, or we have exactly one segment whose length is a power of
			 * two.
			 */
			this.segmentMask = this.key[0].length - 1;
			this.baseMask = this.key.length - 1;
		}

		/**
		 * Creates a new hash big set.
		 *
		 * <p>The actual table size will be the least power of two greater than
		 * <code>expected</code>/<code>f</code>.
		 *
		 * @param expected the expected number of elements in the set.
		 * @param f        the load factor.
		 */
		public Long2IntOpenHashBigMap(final long expected, final float f) {
			if (f <= 0 || f > 1)
				throw new IllegalArgumentException("Load factor must be greater than 0 and smaller than or equal to 1");
			if (this.n < 0) throw new IllegalArgumentException("The expected number of elements must be nonnegative");
			this.f = f;
			this.n = bigArraySize(expected, f);
			this.maxFill = maxFill(this.n, f);
			this.key = LongBigArrays.newBigArray(this.n);
			this.value = IntBigArrays.newBigArray(this.n);
			this.used = BooleanBigArrays.newBigArray(this.n);
			this.initMasks();
		}

		/**
		 * Creates a new hash big set with initial expected {@link Hash#DEFAULT_INITIAL_SIZE} elements
		 * and {@link Hash#DEFAULT_LOAD_FACTOR} as load factor.
		 */

		public Long2IntOpenHashBigMap() {
			this(DEFAULT_INITIAL_SIZE, DEFAULT_LOAD_FACTOR);
		}

		public int put(final long k, final int v) {
			final long h = it.unimi.dsi.fastutil.HashCommon.murmurHash3(k);

			// The starting point.
			int displ = (int) (h & this.segmentMask);
			int base = (int) ((h & this.mask) >>> BigArrays.SEGMENT_SHIFT);

			// There's always an unused entry.
			while (this.used[base][displ]) {
				if (k == this.key[base][displ]) {
					final int oldValue = this.value[base][displ];
					this.value[base][displ] = v;
					return oldValue;
				}
				base = (base + ((displ = (displ + 1) & this.segmentMask) == 0 ? 1 : 0)) & this.baseMask;
			}

			this.used[base][displ] = true;
			this.key[base][displ] = k;
			this.value[base][displ] = v;

			if (++this.size >= this.maxFill) this.rehash(2 * this.n);
			return -1;
		}

		public int get(final long k) {
			final long h = it.unimi.dsi.fastutil.HashCommon.murmurHash3(k);

			// The starting point.
			int displ = (int) (h & this.segmentMask);
			int base = (int) ((h & this.mask) >>> BigArrays.SEGMENT_SHIFT);

			// There's always an unused entry.
			while (this.used[base][displ]) {
				if (k == this.key[base][displ]) return this.value[base][displ];
				base = (base + ((displ = (displ + 1) & this.segmentMask) == 0 ? 1 : 0)) & this.baseMask;
			}

			return -1;
		}

		private void rehash(final long newN) {
			final boolean[][] used = this.used;
			final long[][] key = this.key;
			final int[][] value = this.value;
			final boolean[][] newUsed = BooleanBigArrays.newBigArray(newN);
			final long[][] newKey = LongBigArrays.newBigArray(newN);
			final int[][] newValue = IntBigArrays.newBigArray(newN);
			final long newMask = newN - 1;
			final int newSegmentMask = newKey[0].length - 1;
			final int newBaseMask = newKey.length - 1;

			int base = 0, displ = 0;
			long h;
			long k;

			for (long i = this.size; i-- != 0; ) {

				while (!used[base][displ])
					base = (base + ((displ = (displ + 1) & this.segmentMask) == 0 ? 1 : 0));

				k = key[base][displ];
				h = it.unimi.dsi.fastutil.HashCommon.murmurHash3(k);

				// The starting point.
				int d = (int) (h & newSegmentMask);
				int b = (int) ((h & newMask) >>> BigArrays.SEGMENT_SHIFT);

				while (newUsed[b][d])
					b = (b + ((d = (d + 1) & newSegmentMask) == 0 ? 1 : 0)) & newBaseMask;

				newUsed[b][d] = true;
				newKey[b][d] = k;
				newValue[b][d] = value[base][displ];

				base = (base + ((displ = (displ + 1) & this.segmentMask) == 0 ? 1 : 0));
			}

			this.n = newN;
			this.key = newKey;
			this.value = newValue;
			this.used = newUsed;
			this.initMasks();
			this.maxFill = maxFill(this.n, this.f);
		}

		public void compact() {
			int base = 0, displ = 0, b = 0, d = 0;
			for (long i = this.size; i-- != 0; ) {
				while (!this.used[base][displ])
					base = (base + ((displ = (displ + 1) & this.segmentMask) == 0 ? 1 : 0)) & this.baseMask;
				this.key[b][d] = this.key[base][displ];
				this.value[b][d] = this.value[base][displ];
				base = (base + ((displ = (displ + 1) & this.segmentMask) == 0 ? 1 : 0)) & this.baseMask;
				b = (b + ((d = (d + 1) & this.segmentMask) == 0 ? 1 : 0)) & this.baseMask;
			}
		}

		public long size() {
			return this.size;
		}
	}

	/**
	 * Creates a scattered-arcs ASCII graph.
	 *
	 * @param is an input stream containing a standard scattered list of arcs.
	 */
	public ScatteredLabelledArcsASCIIGraph(final InputStream is) throws IOException {
		this(is, false);
	}

	/**
	 * Creates a scattered-arcs ASCII graph.
	 *
	 * @param is         an input stream containing a standard scattered list of arcs.
	 * @param symmetrize the new graph will be forced to be symmetric.
	 */
	public ScatteredLabelledArcsASCIIGraph(final InputStream is, final boolean symmetrize) throws IOException {
		this(is, symmetrize, false);
	}

	/**
	 * Creates a scattered-arcs ASCII graph.
	 *
	 * @param is         an input stream containing a standard scattered list of arcs.
	 * @param symmetrize the new graph will be forced to be symmetric.
	 * @param noLoops    the new graph will have no loops.
	 */
	public ScatteredLabelledArcsASCIIGraph(final InputStream is, final boolean symmetrize, final boolean noLoops) throws IOException {
		this(is, symmetrize, noLoops, DEFAULT_BATCH_SIZE);
	}

	/**
	 * Creates a scattered-arcs ASCII graph.
	 *
	 * @param is         an input stream containing a standard scattered list of arcs.
	 * @param symmetrize the new graph will be forced to be symmetric.
	 * @param noLoops    the new graph will have no loops.
	 * @param batchSize  the number of integers in a batch; two arrays of integers of this size will be allocated by this method.
	 */
	public ScatteredLabelledArcsASCIIGraph(final InputStream is, final boolean symmetrize, final boolean noLoops, final int batchSize) throws IOException {
		this(is, symmetrize, noLoops, batchSize, null);
	}

	/**
	 * Creates a scattered-arcs ASCII graph.
	 *
	 * @param is         an input stream containing a standard scattered list of arcs.
	 * @param symmetrize the new graph will be forced to be symmetric.
	 * @param noLoops    the new graph will have no loops.
	 * @param batchSize  the number of integers in a batch; two arrays of integers of this size will be allocated by this method.
	 * @param tempDir    a temporary directory for the batches, or <code>null</code> for {@link File#createTempFile(java.lang.String, java.lang.String)}'s choice.
	 */
	public ScatteredLabelledArcsASCIIGraph(final InputStream is, final boolean symmetrize, final boolean noLoops, final int batchSize, final File tempDir) throws IOException {
		this(is, symmetrize, noLoops, batchSize, tempDir, null);
	}

	/**
	 * Creates a scattered-arcs ASCII graph.
	 *
	 * @param is         an input stream containing a standard scattered list of arcs.
	 * @param symmetrize the new graph will be forced to be symmetric.
	 * @param noLoops    the new graph will have no loops.
	 * @param batchSize  the number of integers in a batch; two arrays of integers of this size will be allocated by this method.
	 * @param tempDir    a temporary directory for the batches, or <code>null</code> for {@link File#createTempFile(java.lang.String, java.lang.String)}'s choice.
	 * @param pl         a progress logger, or <code>null</code>.
	 */
	public ScatteredLabelledArcsASCIIGraph(final InputStream is, final boolean symmetrize, final boolean noLoops, final int batchSize, final File tempDir, final ProgressLogger pl) throws IOException {
		this(is, null, null, -1, symmetrize, noLoops, batchSize, tempDir, pl);
	}

	/**
	 * Creates a scattered-arcs ASCII graph.
	 *
	 * @param is       an input stream containing a scattered list of arcs.
	 * @param function an explicitly provided function from string representing nodes to node numbers, or <code>null</code> for the standard behaviour.
	 * @param charset  a character set that will be used to read the identifiers passed to <code>function</code>, or <code>null</code> for ISO-8859-1 (used only if <code>function</code> is not <code>null</code>).
	 * @param n        the number of nodes of the graph (used only if <code>function</code> is not <code>null</code>).
	 */
	public ScatteredLabelledArcsASCIIGraph(final InputStream is, final Object2LongFunction<? extends CharSequence> function, final Charset charset, final int n) throws IOException {
		this(is, function, charset, n, false);
	}

	/**
	 * Creates a scattered-arcs ASCII graph.
	 *
	 * @param is         an input stream containing a scattered list of arcs.
	 * @param function   an explicitly provided function from string representing nodes to node numbers, or <code>null</code> for the standard behaviour.
	 * @param charset    a character set that will be used to read the identifiers passed to <code>function</code>, or <code>null</code> for ISO-8859-1 (used only if <code>function</code> is not <code>null</code>).
	 * @param n          the number of nodes of the graph (used only if <code>function</code> is not <code>null</code>).
	 * @param symmetrize the new graph will be forced to be symmetric.
	 */
	public ScatteredLabelledArcsASCIIGraph(final InputStream is, final Object2LongFunction<? extends CharSequence> function, final Charset charset, final int n, final boolean symmetrize) throws IOException {
		this(is, function, charset, n, symmetrize, false);
	}

	/**
	 * Creates a scattered-arcs ASCII graph.
	 *
	 * @param is         an input stream containing a scattered list of arcs.
	 * @param function   an explicitly provided function from string representing nodes to node numbers, or <code>null</code> for the standard behaviour.
	 * @param charset    a character set that will be used to read the identifiers passed to <code>function</code>, or <code>null</code> for ISO-8859-1 (used only if <code>function</code> is not <code>null</code>).
	 * @param n          the number of nodes of the graph (used only if <code>function</code> is not <code>null</code>).
	 * @param symmetrize the new graph will be forced to be symmetric.
	 * @param noLoops    the new graph will have no loops.
	 */
	public ScatteredLabelledArcsASCIIGraph(final InputStream is, final Object2LongFunction<? extends CharSequence> function, final Charset charset, final int n, final boolean symmetrize, final boolean noLoops) throws IOException {
		this(is, function, charset, n, symmetrize, noLoops, DEFAULT_BATCH_SIZE);
	}

	/**
	 * Creates a scattered-arcs ASCII graph.
	 *
	 * @param is         an input stream containing a scattered list of arcs.
	 * @param function   an explicitly provided function from string representing nodes to node numbers, or <code>null</code> for the standard behaviour.
	 * @param charset    a character set that will be used to read the identifiers passed to <code>function</code>, or <code>null</code> for ISO-8859-1 (used only if <code>function</code> is not <code>null</code>).
	 * @param n          the number of nodes of the graph (used only if <code>function</code> is not <code>null</code>).
	 * @param symmetrize the new graph will be forced to be symmetric.
	 * @param noLoops    the new graph will have no loops.
	 * @param batchSize  the number of integers in a batch; two arrays of integers of this size will be allocated by this method.
	 */
	public ScatteredLabelledArcsASCIIGraph(final InputStream is, final Object2LongFunction<? extends CharSequence> function, final Charset charset, final int n, final boolean symmetrize, final boolean noLoops, final int batchSize) throws IOException {
		this(is, function, charset, n, symmetrize, noLoops, batchSize, null);
	}

	/**
	 * Creates a scattered-arcs ASCII graph.
	 *
	 * @param is         an input stream containing a scattered list of arcs.
	 * @param function   an explicitly provided function from string representing nodes to node numbers, or <code>null</code> for the standard behaviour.
	 * @param charset    a character set that will be used to read the identifiers passed to <code>function</code>, or <code>null</code> for ISO-8859-1 (used only if <code>function</code> is not <code>null</code>).
	 * @param n          the number of nodes of the graph (used only if <code>function</code> is not <code>null</code>).
	 * @param symmetrize the new graph will be forced to be symmetric.
	 * @param noLoops    the new graph will have no loops.
	 * @param batchSize  the number of integers in a batch; two arrays of integers of this size will be allocated by this method.
	 * @param tempDir    a temporary directory for the batches, or <code>null</code> for {@link File#createTempFile(java.lang.String, java.lang.String)}'s choice.
	 */
	public ScatteredLabelledArcsASCIIGraph(final InputStream is, final Object2LongFunction<? extends CharSequence> function, final Charset charset, final int n, final boolean symmetrize, final boolean noLoops, final int batchSize, final File tempDir) throws IOException {
		this(is, function, charset, n, symmetrize, noLoops, batchSize, tempDir, null);
	}

	/**
	 * Creates a scattered-arcs ASCII graph.
	 *
	 * @param is         an input stream containing a scattered list of arcs.
	 * @param function   an explicitly provided function from string representing nodes to node numbers, or <code>null</code> for the standard behaviour.
	 * @param charset    a character set that will be used to read the identifiers passed to <code>function</code>, or <code>null</code> for ISO-8859-1 (used only if <code>function</code> is not <code>null</code>).
	 * @param n          the number of nodes of the graph (used only if <code>function</code> is not <code>null</code>).
	 * @param symmetrize the new graph will be forced to be symmetric.
	 * @param noLoops    the new graph will have no loops.
	 * @param batchSize  the number of integers in a batch; two arrays of integers of this size will be allocated by this method.
	 * @param tempDir    a temporary directory for the batches, or <code>null</code> for {@link File#createTempFile(java.lang.String, java.lang.String)}'s choice.
	 * @param pl         a progress logger, or <code>null</code>.
	 */
	public ScatteredLabelledArcsASCIIGraph(final InputStream is, final Object2LongFunction<? extends CharSequence> function, Charset charset, final int n, final boolean symmetrize, final boolean noLoops, final int batchSize, final File tempDir, final ProgressLogger pl) throws IOException {
		@SuppressWarnings("resource") final FastBufferedInputStream fbis = new FastBufferedInputStream(is);
		ScatteredLabelledArcsASCIIGraph.Long2IntOpenHashBigMap map = new ScatteredLabelledArcsASCIIGraph.Long2IntOpenHashBigMap();

		int numNodes = -1;
		if (charset == null) charset = StandardCharsets.ISO_8859_1;

		int j;
		int[] source = new int[batchSize], target = new int[batchSize];
		Label[] labels = new Label[batchSize];
		final ObjectArrayList<File> batches = new ObjectArrayList<>();

		if (pl != null) {
			pl.itemsName = "labelled arcs";
			pl.start("Creating sorted batches...");
		}

		j = 0;
		long pairs = 0; // Number of pairs
		byte[] array = new byte[1024];
		for (long line = 1; ; line++) {
			int start = 0, len;
			while ((len = fbis.readLine(array, start, array.length - start, FastBufferedInputStream.ALL_TERMINATORS)) == array.length - start) {
				start += len;
				array = ByteArrays.grow(array, array.length + 1);
			}

			if (len == -1) break; // EOF

			final int lineLength = start + len;

			if (DEBUG)
				System.err.println("Reading line " + line + "... (" + new String(array, 0, lineLength, charset) + ")");

			// Skip whitespace at the start of the line.
			int offset = 0;
			while (offset < lineLength && array[offset] >= 0 && array[offset] <= ' ') offset++;

			if (offset == lineLength) {
				if (DEBUG) System.err.println("Skipping line " + line + "...");
				continue; // Whitespace line
			}

			if (array[0] == '#') continue;

			// Scan source id.
			start = offset;
			while (offset < lineLength && (array[offset] < 0 || array[offset] > ' ')) offset++;

			int s;

			if (function == null) {
				final long sl;
				try {
					sl = getLong(array, start, offset - start);
				} catch (final RuntimeException e) {
					// Discard up to the end of line
					LOGGER.error("Error at line " + line + ": " + e.getMessage());
					continue;
				}

				s = map.get(sl);
				if (s == -1) map.put(sl, s = (int) map.size());

				if (DEBUG) System.err.println("Parsed source at line " + line + ": " + sl + " => " + s);
			} else {
				final String ss = new String(array, start, offset - start, charset);
				final long sl = function.getLong(ss);
				if (sl == -1) {
					LOGGER.warn("Unknown source identifier " + ss + " at line " + line);
					continue;
				}
				if (sl < 0 || sl >= n)
					throw new IllegalArgumentException("Source node number out of range for node " + ss + ": " + sl);
				s = (int) sl;
				if (DEBUG) System.err.println("Parsed target at line " + line + ": " + ss + " => " + s);
			}


			// Skip whitespace between identifiers.
			while (offset < lineLength && array[offset] >= 0 && array[offset] <= ' ') offset++;

			if (offset == lineLength) {
				LOGGER.error("Error at line " + line + ": no target");
				continue;
			}

			// Scan target id.
			start = offset;
			while (offset < lineLength && (array[offset] < 0 || array[offset] > ' ')) offset++;

			int t;

			if (function == null) {
				final long tl;
				try {
					tl = getLong(array, start, offset - start);
				} catch (final RuntimeException e) {
					// Discard up to the end of line
					LOGGER.error("Error at line " + line + ": " + e.getMessage());
					continue;
				}

				t = map.get(tl);
				if (t == -1) map.put(tl, t = (int) map.size());

				if (DEBUG) System.err.println("Parsed target at line " + line + ": " + tl + " => " + t);
			} else {
				final String ts = new String(array, start, offset - start, charset);
				final long tl = function.getLong(ts);
				if (tl == -1) {
					LOGGER.warn("Unknown target identifier " + ts + " at line " + line);
					continue;
				}

				if (tl < 0 || tl >= n)
					throw new IllegalArgumentException("Target node number out of range for node " + ts + ": " + tl);
				t = (int) tl;
				if (DEBUG) System.err.println("Parsed target at line " + line + ": " + ts + " => " + t);
			}

			// Diciamo di avere una funzione da stringe in Label
			final Object2ObjectFunction<? extends CharSequence, Label> labelFunction = new Object2ObjectArrayMap<>();

			// Scan label

			// usa la funzione se c'è, altrimenti costruisci una label con chiave vuota e valore stringa (?)

			start = offset;
			while (offset < lineLength && (array[offset] < 0 || array[offset] > ' ')) offset++;

			Label l;

			final String ls = new String(array, start, offset - start, charset);
			final Label label = labelFunction.get(ls);
			if (label == labelFunction.defaultReturnValue()) {
				LOGGER.warn("Unknown label " + ls + " at line " + line);
				continue;
			}

			l = label;
			if (DEBUG) System.err.println("Parsed label at line " + line + ": " + ts + " => " + t);

			if (function == null) {
				final long tl;
				try {
					tl = getLong(array, start, offset - start);
				} catch (final RuntimeException e) {
					// Discard up to the end of line
					LOGGER.error("Error at line " + line + ": " + e.getMessage());
					continue;
				}

				t = map.get(tl);
				if (t == -1) map.put(tl, t = (int) map.size());

				if (DEBUG) System.err.println("Parsed target at line " + line + ": " + tl + " => " + t);
			} else {
				final String ls = new String(array, start, offset - start, charset);
				final Label label = labelFunction.get(ls);
				if (label == labelFunction.defaultReturnValue()) {
					LOGGER.warn("Unknown label " + ls + " at line " + line);
					continue;
				}

				l = label;
				if (DEBUG) System.err.println("Parsed label at line " + line + ": " + ts + " => " + t);
			}







			// Skip whitespace after target.
			while (offset < lineLength && array[offset] >= 0 && array[offset] <= ' ') offset++;

			if (offset < lineLength) LOGGER.warn("Trailing characters ignored at line " + line);

			if (DEBUG) System.err.println("Parsed arc at line " + line + ": " + s + " -> " + t);

			if (s != t || !noLoops) {
				source[j] = s;
				target[j++] = t;

				if (j == batchSize) {
					pairs += Transform.processBatch(batchSize, source, target, tempDir, batches);
					j = 0;
				}

				if (symmetrize && s != t) {
					source[j] = t;
					target[j++] = s;
					if (j == batchSize) {
						pairs += Transform.processBatch(batchSize, source, target, tempDir, batches);
						j = 0;
					}
				}

				if (pl != null) pl.lightUpdate();
			}
		}

		if (j != 0) pairs += Transform.processBatch(j, source, target, tempDir, batches);

		if (pl != null) {
			pl.done();
			// Transform.logBatches(batches, pairs, pl);
			logBatches(batches, pairs, pl);
		}

		numNodes = function == null ? (int) map.size() : function.size();
		source = null;
		target = null;

		map.compact();

		final File keyFile = File.createTempFile(ScatteredLabelledArcsASCIIGraph.class.getSimpleName(), "keys", tempDir);
		keyFile.deleteOnExit();
		final File valueFile = File.createTempFile(ScatteredLabelledArcsASCIIGraph.class.getSimpleName(), "values", tempDir);
		valueFile.deleteOnExit();

		BinIO.storeLongs(map.key, 0, map.size(), keyFile);
		BinIO.storeInts(map.value, 0, map.size(), valueFile);

		map = null;

		long[][] key = BinIO.loadLongsBig(keyFile);
		keyFile.delete();
		int[][] value = BinIO.loadIntsBig(valueFile);
		valueFile.delete();

		if (function == null) {
			this.ids = new long[numNodes];

			final long[] result = new long[numNodes];
			for (int i = numNodes; i-- != 0; ) result[BigArrays.get(value, i)] = BigArrays.get(key, i);
			this.ids = result;
		}

		key = null;
		value = null;

		this.batchGraph = new Transform.BatchGraph(function == null ? numNodes : n, pairs, batches);
	}

	private final static long getLong(final byte[] array, int offset, int length) {
		if (length == 0) throw new NumberFormatException("Empty number");
		int sign = 1;
		if (array[offset] == '-') {
			sign = -1;
			offset++;
			length--;
		}

		long value = 0;
		for (int i = 0; i < length; i++) {
			final byte digit = array[offset + i];
			if (digit < '0' || digit > '9') throw new NumberFormatException("Not a digit: " + (char) digit);
			value *= 10;
			value += digit - '0';
		}

		return sign * value;
	}

	/**
	 * Creates a scattered-arcs ASCII graph.
	 *
	 * @param arcs       an iterator returning the arcs as two-element arrays.
	 * @param symmetrize the new graph will be forced to be symmetric.
	 * @param noLoops    the new graph will have no loops.
	 * @param batchSize  the number of integers in a batch; two arrays of integers of this size will be allocated by this method.
	 * @param tempDir    a temporary directory for the batches, or <code>null</code> for {@link File#createTempFile(java.lang.String, java.lang.String)}'s choice.
	 * @param pl         a progress logger, or <code>null</code>.
	 */
	public ScatteredLabelledArcsASCIIGraph(final Iterator<long[]> arcs, final Iterator<Label> labels, final boolean symmetrize,
	                                       final boolean noLoops, final int batchSize, final File tempDir, final ProgressLogger pl) throws IOException {
		ScatteredLabelledArcsASCIIGraph.Long2IntOpenHashBigMap map = new ScatteredLabelledArcsASCIIGraph.Long2IntOpenHashBigMap();

		int numNodes = -1;

		int j;
		int[] source = new int[batchSize], target = new int[batchSize];
		Label[] arcLabels = new Label[batchSize];
		final ObjectArrayList<File> batches = new ObjectArrayList<>();

		if (pl != null) {
			pl.itemsName = "labelled arcs";
			pl.start("Creating sorted batches...");
		}

		j = 0;
		long pairs = 0; // Number of pairs
		while (arcs.hasNext()) {
			final long[] arc = arcs.next();
			final long sl = arc[0];
			int s = map.get(sl);
			if (s == -1) map.put(sl, s = (int) map.size());
			final long tl = arc[1];
			int t = map.get(tl);
			if (t == -1) map.put(tl, t = (int) map.size());
			Label label = labels.next();

			if (s != t || !noLoops) {
				source[j] = s;
				target[j] = t;
				arcLabels[j++] = label;

				if (j == batchSize) {
					// ordina source riflettendo la permutazione su target e arcLabels (?????)
					pairs += Transform.processBatch(batchSize, source, target, tempDir, batches);
					j = 0;
				}

				if (symmetrize && s != t) {
					source[j] = t;
					target[j] = s;
					arcLabels[j++] = label;

					if (j == batchSize) {
						// ordina source riflettendo la permutazione su target e arcLabels (?????)
						pairs += Transform.processBatch(batchSize, source, target, tempDir, batches);
						j = 0;
					}
				}

				if (pl != null) pl.lightUpdate();
			}
		}

		// ordina source riflettendo la permutazione su target e arcLabels (?????)
		if (j != 0) pairs += Transform.processBatch(j, source, target, tempDir, batches);

		if (pl != null) {
			pl.done();
			// Transform.logBatches(batches, pairs, pl);
			logBatches(batches, pairs, pl);
		}

		numNodes = (int) map.size();
		source = null;
		target = null;

		map.compact();

		final File keyFile = File.createTempFile(ScatteredLabelledArcsASCIIGraph.class.getSimpleName(), "keys", tempDir);
		keyFile.deleteOnExit();
		final File valueFile = File.createTempFile(ScatteredLabelledArcsASCIIGraph.class.getSimpleName(), "values", tempDir);
		valueFile.deleteOnExit();

		BinIO.storeLongs(map.key, 0, map.size(), keyFile);
		BinIO.storeInts(map.value, 0, map.size(), valueFile);

		map = null;

		long[][] key = BinIO.loadLongsBig(keyFile);
		keyFile.delete();
		int[][] value = BinIO.loadIntsBig(valueFile);
		valueFile.delete();

		this.ids = new long[numNodes];

		final long[] result = new long[numNodes];
		for (int i = numNodes; i-- != 0; ) result[BigArrays.get(value, i)] = BigArrays.get(key, i);
		this.ids = result;

		key = null;
		value = null;

		this.batchGraph = new Transform.BatchGraph(numNodes, pairs, batches);
	}

	@Override
	public int numNodes() {
		if (this.batchGraph == null)
			throw new UnsupportedOperationException("The number of nodes is unknown (you need to exhaust the input)");
		return this.batchGraph.numNodes();
	}

	@Override
	public long numArcs() {
		if (this.batchGraph == null)
			throw new UnsupportedOperationException("The number of arcs is unknown (you need to exhaust the input)");
		return this.batchGraph.numArcs();
	}

	@Override
	public NodeIterator nodeIterator(final int from) {
		return this.batchGraph.nodeIterator(from);
	}

	@Override
	public boolean hasCopiableIterators() {
		return this.batchGraph.hasCopiableIterators();
	}

	@Override
	public ScatteredLabelledArcsASCIIGraph copy() {
		return this;
	}

	@SuppressWarnings("unchecked")
	public static void main(final String[] args) throws IllegalArgumentException, SecurityException, IOException, JSAPException, ClassNotFoundException {
		String basename;
		final SimpleJSAP jsap = new SimpleJSAP(ScatteredLabelledArcsASCIIGraph.class.getName(), "Converts a scattered list of arcs from standard input into a BVGraph. The list of" +
				"identifiers in order of appearance will be saved with extension \"" + IDS_EXTENSION + "\", unless a translation function has been specified.",
				new Parameter[]{
						new FlaggedOption("logInterval", JSAP.LONG_PARSER, Long.toString(ProgressLogger.DEFAULT_LOG_INTERVAL), JSAP.NOT_REQUIRED, 'l', "log-interval", "The minimum time interval between activity logs in milliseconds."),
						new FlaggedOption("batchSize", JSAP.INTSIZE_PARSER, Integer.toString(DEFAULT_BATCH_SIZE), JSAP.NOT_REQUIRED, 's', "batch-size", "The maximum size of a batch, in arcs."),
						new FlaggedOption("tempDir", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.NOT_REQUIRED, 'T', "temp-dir", "A directory for all temporary batch files."),
						new Switch("symmetrize", 'S', "symmetrize", "Force the output graph to be symmetric."),
						new Switch("noLoops", 'L', "no-loops", "Remove loops from the output graph."),
						new Switch("zipped", 'z', "zipped", "The string list is compressed in gzip format."),
						new FlaggedOption("function", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.NOT_REQUIRED, 'f', "function", "A serialised function from strings to longs that will be used to translate identifiers to node numbers."),
						new FlaggedOption("charset", JSAP.STRING_PARSER, "ISO-8859-1", JSAP.NOT_REQUIRED, 'C', "charset", "The charset used to read the list of arcs."),
						new FlaggedOption("n", JSAP.INTSIZE_PARSER, JSAP.NO_DEFAULT, JSAP.NOT_REQUIRED, 'n', "n", "The number of nodes of the graph (only if you specified a function that does not return the size of the key set, or if you want to override that size)."),
						new FlaggedOption("comp", JSAP.STRING_PARSER, null, JSAP.NOT_REQUIRED, 'c', "comp", "A compression flag (may be specified several times).").setAllowMultipleDeclarations(true),
						new FlaggedOption("windowSize", JSAP.INTEGER_PARSER, String.valueOf(BVGraph.DEFAULT_WINDOW_SIZE), JSAP.NOT_REQUIRED, 'w', "window-size", "Reference window size (0 to disable)."),
						new FlaggedOption("maxRefCount", JSAP.INTEGER_PARSER, String.valueOf(BVGraph.DEFAULT_MAX_REF_COUNT), JSAP.NOT_REQUIRED, 'm', "max-ref-count", "Maximum number of backward references (-1 for ∞)."),
						new FlaggedOption("minIntervalLength", JSAP.INTEGER_PARSER, String.valueOf(BVGraph.DEFAULT_MIN_INTERVAL_LENGTH), JSAP.NOT_REQUIRED, 'i', "min-interval-length", "Minimum length of an interval (0 to disable)."),
						new FlaggedOption("zetaK", JSAP.INTEGER_PARSER, String.valueOf(BVGraph.DEFAULT_ZETA_K), JSAP.NOT_REQUIRED, 'k', "zeta-k", "The k parameter for zeta-k codes."),
						new UnflaggedOption("basename", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, JSAP.NOT_GREEDY, "The basename of the output graph"),
				}
		);

		final JSAPResult jsapResult = jsap.parse(args);
		if (jsap.messagePrinted()) System.exit(1);

		basename = jsapResult.getString("basename");

		int flags = 0;
		for (final String compressionFlag : jsapResult.getStringArray("comp"))
			try {
				flags |= BVGraph.class.getField(compressionFlag).getInt(BVGraph.class);
			} catch (final Exception notFound) {
				throw new JSAPException("Compression method " + compressionFlag + " unknown.");
			}

		final int windowSize = jsapResult.getInt("windowSize");
		final int zetaK = jsapResult.getInt("zetaK");
		int maxRefCount = jsapResult.getInt("maxRefCount");
		if (maxRefCount == -1) maxRefCount = Integer.MAX_VALUE;
		final int minIntervalLength = jsapResult.getInt("minIntervalLength");

		Object2LongFunction<String> function = null;
		Charset charset = null;
		int n = -1;
		if (jsapResult.userSpecified("function")) {
			function = (Object2LongFunction<String>) BinIO.loadObject(jsapResult.getString("function"));
			charset = Charset.forName(jsapResult.getString("charset"));
			if (function.size() == -1) {
				if (!jsapResult.userSpecified("n"))
					throw new IllegalArgumentException("You must specify a graph size if you specify a translation function that does not return the size of the key set.");
				n = jsapResult.getInt("n");
			} else n = function.size();
		}

		File tempDir = null;
		if (jsapResult.userSpecified("tempDir")) tempDir = new File(jsapResult.getString("tempDir"));

		final ProgressLogger pl = new ProgressLogger(LOGGER, jsapResult.getLong("logInterval"), TimeUnit.MILLISECONDS);
		final boolean zipped = jsapResult.getBoolean("zipped");
		final InputStream inStream = (zipped ? new GZIPInputStream(System.in) : System.in);
		final ScatteredLabelledArcsASCIIGraph graph = new ScatteredLabelledArcsASCIIGraph(inStream, function, charset, n, jsapResult.userSpecified("symmetrize"), jsapResult.userSpecified("noLoops"), jsapResult.getInt("batchSize"), tempDir, pl);
		BVGraph.store(graph, basename, windowSize, maxRefCount, minIntervalLength, zetaK, flags, pl);
		if (function == null) BinIO.storeLongs(graph.ids, basename + IDS_EXTENSION);
	}


	// può essere cancellato perchè è package-private, l'ho copiato per fare prima.
	protected static void logBatches(final ObjectArrayList<File> batches, final long pairs, final ProgressLogger pl) {
		long length = 0;
		for (final File f : batches) length += f.length();
		pl.logger().info("Created " + batches.size() + " batches using " + ((double) Byte.SIZE * length / pairs) + " bits/arc.");
	}
}

