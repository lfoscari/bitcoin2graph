package it.unimi.dsi.law;

import com.martiansoftware.jsap.JSAPException;
import it.unimi.dsi.webgraph.algo.HyperBall;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;

import static it.unimi.dsi.law.Parameters.*;

public class Centralities {
	public static void main(String[] args) throws JSAPException, IOException, ClassNotFoundException, InvocationTargetException, IllegalAccessException, InstantiationException, NoSuchMethodException {
		// -server -Xss256K -Xms100G -Xmx800G -XX:PretenureSizeThreshold=512M -XX:MaxNewSize=4G \
		//      -XX:+UseNUMA -XX:+UseTLAB -XX:+ResizeTLAB \
		//      -XX:+UseConcMarkSweepGC -XX:CMSInitiatingOccupancyFraction=99 -XX:+UseCMSInitiatingOccupancyOnly \
		//      -verbose:gc -Xloggc:gc.log
		HyperBall.main("-l %d -n %s -d %s -h %s -z %s -c %s -L %s -N %s -r %s -S 024 %s".formatted(
				100,
				neighbourhoodFunctionFile,
				sumOfDistancesFile,
				harmonicCentralityFile,
				discountedGainCentralityFile,
				closenessCentralityFile,
				linCentralityFile,
				nieminenCentralityFile,
				reachableFile,
				basename
		).split(" "));
	}
}
