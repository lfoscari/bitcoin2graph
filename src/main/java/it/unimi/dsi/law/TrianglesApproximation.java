package it.unimi.dsi.law;

import com.martiansoftware.jsap.JSAPException;
import es.yrbcn.graph.triangles.RunTriangles;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;

public class TrianglesApproximation {
	// A simple wrapper for RunTriangles
	public static void main(String[] args) throws JSAPException, IOException, ClassNotFoundException, InvocationTargetException, IllegalAccessException, NoSuchMethodException {
		RunTriangles.main(args);
	}
}
