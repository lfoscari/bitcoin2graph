package it.unimi.dsi.law;

import it.unimi.dsi.webgraph.labelling.ArcLabelledNodeIterator;
import it.unimi.dsi.webgraph.labelling.GammaCodedIntLabel;
import it.unimi.dsi.webgraph.labelling.Label;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Field;

public class TMP {
	public static void main(String[] args) {
		String s = """
0 1 902
0 2 902
0 3 903
1 2 912
1 3 913""";

		try (InputStream is = new ByteArrayInputStream(s.getBytes())) {
			ScatteredLabelledArcsASCIIGraph.LabelMapping lmap = (label, st) -> {
				int n = Integer.parseInt(st);
				try {
					Field f = label.getClass().getField("value");
					f.setAccessible(true);
					f.set(label, n);
				} catch (NoSuchFieldException | IllegalAccessException e) {
					throw new RuntimeException(e);
				}
			};

			ScatteredLabelledArcsASCIIGraph g = new ScatteredLabelledArcsASCIIGraph(
					is, new GammaCodedIntLabel("FOO"), lmap, false, false, 10, null, null);

			ArcLabelledNodeIterator nodeIterator = g.nodeIterator();
			while (nodeIterator.hasNext()) {
				int node = nodeIterator.nextInt();
				int[] successors = nodeIterator.successorArray();
				Label[] labels = nodeIterator.labelArray();

				for (int i = 0; i < nodeIterator.outdegree(); i++) {
					System.out.println(node + " -> " + successors[i] + " (" + labels[i].get() + ")");
				}
			}
		} catch (IOException e) {
			throw new RuntimeException(e);
		}

	}
}
