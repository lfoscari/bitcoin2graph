package it.unimi.dsi.law;

import org.jgrapht.*;
import org.jgrapht.graph.*;
import org.jgrapht.nio.*;
import org.jgrapht.nio.dot.*;

import it.unimi.dsi.webgraph.BVGraph;

import java.io.*;
import java.net.*;
import java.util.*;

public class Webgraph2Graphviz {
	public static void main(String[] args) throws URISyntaxException, IOException {
        BVGraph webgraph = BVGraph.load(Blockchain2Webgraph.defaultLocation + "webgraph/bitcoin");
        Graph<Integer, DefaultEdge> graphviz = new DefaultDirectedGraph<>(DefaultEdge.class);

        for (int i = 0; i < webgraph.numNodes(); i++) {
            graphviz.addVertex(i);
        }

        webgraph.nodeIterator().forEachRemaining((node) -> {
            int[] successors = webgraph.successorArray(node);
            for (int i = 0; i < successors.length; i++) {
                graphviz.addEdge(node, successors[i]);
            }
        });

		DOTExporter<Integer, DefaultEdge> exporter = new DOTExporter<>();

        exporter.setVertexAttributeProvider((v) -> {
            Map<String, Attribute> map = new LinkedHashMap<>();
            map.put("label", DefaultAttribute.createAttribute(v.toString()));
            return map;
        });

        File dot = new File(Blockchain2Webgraph.defaultLocation + "webgraph/bitcoin.dot");
        FileWriter dotWriter = new FileWriter(dot);

        exporter.exportGraph(graphviz, dotWriter);
        System.out.println("Dot file exported in " + Blockchain2Webgraph.defaultLocation + "webgraph/bitcoin.dot");
	}
}
