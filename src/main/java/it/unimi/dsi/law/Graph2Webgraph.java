package it.unimi.dsi.law;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;

import com.martiansoftware.jsap.JSAPException;

import it.unimi.dsi.webgraph.ASCIIGraph;
import it.unimi.dsi.webgraph.BVGraph;

public class Graph2Webgraph {
	public static void main(String[] args) throws IOException, IllegalAccessException, InvocationTargetException, NoSuchMethodException, JSAPException, ClassNotFoundException, InstantiationException {
		String destination = Blockchain2Graph.defaultLocation + "webgraph/";
		File destinationFolder = new File(destination);

		destinationFolder.mkdir();
		File ascii = new File(Blockchain2Graph.defaultLocation + "ascii.graph-txt");

		System.out.println("Converting to BVGraph");

		ASCIIGraph graph = ASCIIGraph.loadOnce(new FileInputStream(ascii));
		BVGraph.store(graph, destination + "bitcoin");
	}
}
