package it.unimi.dsi.law;

import it.unimi.dsi.webgraph.BVGraph;
import org.jgrapht.Graph;
import org.jgrapht.graph.DefaultDirectedGraph;
import org.jgrapht.graph.DefaultEdge;
import org.jgrapht.nio.Attribute;
import org.jgrapht.nio.DefaultAttribute;
import org.jgrapht.nio.dot.DOTExporter;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;

public class Webgraph2Graphviz {
    public static final String graphBasename = Parameters.resources + "ArrayListMutableGraph/bitcoin";

    public static void main(String[] args) throws IOException {
        BVGraph webgraph = BVGraph.load(graphBasename);
        Graph<Integer, DefaultEdge> graphviz = new DefaultDirectedGraph<>(DefaultEdge.class);

        for (int i = 0; i < webgraph.numNodes(); i++)
            graphviz.addVertex(i);

        webgraph.nodeIterator().forEachRemaining((node) -> {
            int[] successors = webgraph.successorArray(node);
            for (int successor : successors)
                graphviz.addEdge(node, successor);
        });

        DOTExporter<Integer, DefaultEdge> exporter = new DOTExporter<>();

        exporter.setVertexAttributeProvider((v) -> {
            Map<String, Attribute> map = new LinkedHashMap<>();
            map.put("label", DefaultAttribute.createAttribute(v.toString()));
            return map;
        });

        File dot = new File(graphBasename + ".dot");
        FileWriter dotWriter = new FileWriter(dot);
        exporter.exportGraph(graphviz, dotWriter);

        System.out.println(
                "Dot file exported in " + dot.getPath() + "\n" +
                        "To visualize run (it may take a lot!)" + "\n" +
                        "$ dot -Tpdf " + dot.getPath() + " > " + graphBasename + ".pdf"
        );
    }
}
