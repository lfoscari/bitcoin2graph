package it.unimi.dsi.law;

import static org.junit.Assert.assertEquals;
import org.junit.Test;

public class ClusteringCoefficientTest {
	@PersonDegrees
	public void clique() {
		CliqueGraph cg = new CliqueGraph(1000);
		assertEquals(1., ClusteringCoefficient.computeGlobalClusteringCoefficient(cg, .5), 0.01);
	}
}
