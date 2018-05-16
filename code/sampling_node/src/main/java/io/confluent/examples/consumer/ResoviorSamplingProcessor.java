package io.confluent.examples.consumer;

import com.opencsv.CSVWriter;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.AbstractProcessor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;

import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.Random;

/**
 * Created by zhenyuwen on 25/08/2017.
 */
public class ResoviorSamplingProcessor extends AbstractProcessor<String, String> {

    private ProcessorContext context;
    private LinkedList<String> kvStore;
    private int currentCounter = 0;
    private double currentWeight = 1.0;
    private int Counter = 0;
    private double Weight = 1.0;
    private int chunk_num = 0;
    private int sampleSize = 40000;
    private int window_size = 5000;
    private boolean isfinished = false;
    private  String storeName;
    private int win_index = 0;
    private long startTime =0;
    private int total =0;
    private ArrayList<String[]> record;


    public ResoviorSamplingProcessor(String storeName){
        this.storeName = storeName;
    }
    public void process(String key, String value) {

        if (startTime == 0){
            startTime = System.currentTimeMillis();
        }
        if ((System.currentTimeMillis()-startTime)>= window_size){
            startTime =0;
            if(isfinished){

            }else {
                if(chunk_num>0){
                    Weight = weightComputing();
                    Counter = kvStore.size();
                    sendToTopic(false);
                   }
            }
            win_index++;
        }

        if (!value.contains("C:") && !value.contains("W:") && !value.equals("last")){
            chunk_num++;
            total++;
            Sampling(value);
        }
        if(value.contains("C:")){
            String parts[] = value.split(":");
            currentCounter = Integer.valueOf(parts[1]);
        }
        if(value.contains("W:")){
            if (chunk_num>0){
                Weight = weightComputing();
                Counter = kvStore.size();
                sendToTopic(false);
            }
            String parts[] = value.split(":");
            currentWeight = Double.valueOf(parts[1]);
        }
        if (value.equals("last")){
            if(chunk_num>0){
                Weight = weightComputing();
                Counter =kvStore.size();
            }
            System.out.println(storeName+" level_1_last");
            sendToTopic(true);
            win_index++;
        }
    }

    @Override
    public void init(ProcessorContext context) {
        this.context = context;
        this.kvStore = new LinkedList<>();
        this.record = new ArrayList<>();
    }

    void sendToTopic(boolean isFinal){
        if(chunk_num>0){

            context.forward("data", "W:"+String.valueOf(Weight));
            context.forward("data", "C:"+String.valueOf(Counter));


            for (String item : kvStore){
                context.forward("data", item);
            }

        }

        if(isFinal){
            context.forward("data", "last");
            isfinished = true;
        }
        context.commit();
        Weight = 1.0;
        Counter = 0;
        chunk_num = 0;
        total=0;
        kvStore.clear();

    }

//    this function will be triggered when a window time has finished
    @Override
    public void punctuate(long streamTime) {

    }

    private double weightComputing(){
        double update_carried_wt =currentWeight;
        double window_weight = 1.0;
        if (chunk_num>sampleSize){
            window_weight = chunk_num/(double) sampleSize;
        }
        double wt = window_weight*update_carried_wt;
        double sampleRate=1;
        if (chunk_num>sampleSize){
            sampleRate = sampleSize/(double) chunk_num;
        }

        String [] item = new String[2];
        item[0] = String.valueOf(chunk_num);
        item[1] = String.valueOf(sampleRate);
        record.add(item.clone());
        return  wt;
    }

    void Sampling(String value){
        if(kvStore.size()<sampleSize){
            kvStore.add(value);
        }else {
            int position = getRandomNumberInRange(0, total);
            if (position<sampleSize-1){
                kvStore.set(position,value);
            }
        }
    }

    int getRandomNumberInRange(int min, int max) {

        if (min >= max) {
            throw new IllegalArgumentException("max must be greater than min");
        }

        Random r = new Random();
        return r.nextInt((max - min) + 1) + min;
    }
}
