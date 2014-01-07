package com.bah.externship;

import org.apache.commons.math3.distribution.NormalDistribution;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class NormsInvTest {

    double probability = 0.95;

    @Test
    public void normalDistributionTest() {
        NormalDistribution dist = new NormalDistribution();
        double result = dist.inverseCumulativeProbability(probability);
        System.out.println(result);
        assertEquals(result, 1.644853627, 0.00000001);
    }
}
