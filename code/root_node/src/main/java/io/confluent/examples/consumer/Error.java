package io.confluent.examples.consumer;
import org.apache.commons.math3.distribution.NormalDistribution;

import static java.lang.Math.sqrt;

/**
 * Created by zhenyuwen on 16/05/2018.
 */
public class Error {

    public double getCriticalValueNorm(double p){
        double value = new NormalDistribution().inverseCumulativeProbability(p);
        return value;
    }

    public double getconfidenceInterval(double n, double std){
        double conf_level = 0.95;
        return getCriticalValueNorm(1 - conf_level / 2) * std / sqrt(n);
    }
}
