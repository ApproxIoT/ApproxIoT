package io.confluent.examples.consumer;

import org.apache.kafka.streams.processor.AbstractProcessor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;

import java.util.ArrayList;
import java.util.Random;

/**
 * Created by zhenyuwen on 12/09/2017.
 */
public class SRSProcessor extends AbstractProcessor<String, String> {
    private Random rand;
    private double sampleRate = 0.1;
    private ProcessorContext context;
    private String streamName;
    private long commitTime=0;
    private int commitInvertal=1000;


    public SRSProcessor(String streamName){
        this.streamName = streamName;
    }
    public void process(String key, String value) {
        if (commitTime ==0){
            commitTime = System.currentTimeMillis();
        }

        if ((System.currentTimeMillis()-commitTime)>= commitInvertal){
            commitTime=0;
            context.commit();
        }

        if (!value.equals("last")){
            double coin = rand.nextDouble();
            if (coin <= sampleRate){
                context.forward("data", value);
            }
        }

        if (value.equals("last")){
            context.commit();
            System.out.println(streamName+" first level last");
            context.forward("data", "last");
        }

    }

    @Override
    public void init(ProcessorContext context) {
        this.context = context;
        this.rand = new Random();

    }

    @Override
    public void punctuate(long streamTime) {
    }
}
