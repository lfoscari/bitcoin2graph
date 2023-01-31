package it.unimi.dsi.law;

import it.unimi.dsi.fastutil.io.BinIO;
import it.unimi.dsi.lang.MutableString;
import it.unimi.dsi.sux4j.mph.GOVMinimalPerfectHashFunction;

import java.io.IOException;

public class TMP {
    public static void main(String[] args) throws IOException, ClassNotFoundException {
        GOVMinimalPerfectHashFunction<MutableString> map = (GOVMinimalPerfectHashFunction<MutableString>) BinIO.loadObject(Parameters.addressesMap.toFile());

        for (MutableString address: Utils.readTSVs(Parameters.addresses.toFile(), new MutableString(), null)) {
            System.out.println(address + " => " + map.getLong(address));
        }
    }
}
