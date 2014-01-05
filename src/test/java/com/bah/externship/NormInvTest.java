package com.bah.externship;

import org.apache.commons.math3.distribution.NormalDistribution;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class NormInvTest {

    double probability = 0.01;
    double mean = 0.152;
    double standardDeviation = 0.135;

    @Test
    public void normalDistributionTest() {
        NormalDistribution dist = new NormalDistribution(mean, standardDeviation);
        double result = dist.inverseCumulativeProbability(probability);
        System.out.println(result);
        assertEquals(-0.162, result, 0.0001);
    }
}
