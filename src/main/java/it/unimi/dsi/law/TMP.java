package it.unimi.dsi.law;

import java.util.ArrayList;
import java.util.List;

public class TMP {
    public static void main(String[] args) {
        ArrayList<Integer> a = new ArrayList<>(List.of(1, 2, 3, 4, 5, 6, 7, 8, 9, 10));
        List<Integer> b = a.subList(0, 4);

        System.out.println(a + " - " + b);

        b.clear();

        System.out.println(a + " - " + b);
    }
}
