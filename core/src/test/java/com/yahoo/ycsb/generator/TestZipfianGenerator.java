package com.yahoo.ycsb.generator;

import org.testng.annotations.Test;
import static org.testng.AssertJUnit.assertTrue;

public class TestZipfianGenerator {
    @Test
    public void testMinParameter() {
        long min = 5;
        long max = 100;
        Boolean gr_max = true;

        ZipfianGenerator zipfian = new ZipfianGenerator(min, max);

        for (int i = 0; i < 1000; i++) {
            long rnd = zipfian.nextLong();
            // System.out.println(rnd);
            if(rnd < min) gr_max = false;
        }

        assertTrue(gr_max);
    }
}
