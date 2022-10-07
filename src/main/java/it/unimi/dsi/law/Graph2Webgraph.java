package it.unimi.dsi.law;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;

import com.martiansoftware.jsap.JSAPException;

import it.unimi.dsi.webgraph.BVGraph;

public class Graph2Webgraph {
	public static void main(String[] args) throws IOException, IllegalAccessException, InvocationTargetException, NoSuchMethodException, JSAPException, ClassNotFoundException, InstantiationException {
		System.out.println("Converting to BVGraph");
		
		(new File(Blockchain2Graph.defaultLocation + "webgraph/")).mkdir();
		BVGraph.main(new String[] {
			"-g", "ASCIIGraph",
			Blockchain2Graph.defaultLocation + "ascii",
			Blockchain2Graph.defaultLocation + "webgraph/bitcoin"
		});
	}
}
