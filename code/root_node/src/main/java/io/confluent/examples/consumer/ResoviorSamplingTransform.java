package io.confluent.examples.consumer;

import com.opencsv.CSVWriter;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;

import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.Random;

/**
 * Created by zhenyuwen on 29/08/2017.
 */
public class ResoviorSamplingTransform implements Transformer<String, String, KeyValue<String, String>> {
    private ProcessorContext context;
    private LinkedList<String> kvStore;
    private int currentCounter = 0;
    private double currentWeight = 1.0;
    private int Counter = 0;
    private double Weight = 1.0;
    private int chunk_num = 0;
    private int sampleSize = 500000;
    private int window_size = 5000;
    private  String storeName;
    private int win_index = 0;
    private boolean isfinished = false;
    private long startTime =0;
    private ArrayList<String[]> record;
    private long commitTime=0;
    private int commitInvertal=1000;
    private int total=0;
    private int oneWindowNum =0;

    public ResoviorSamplingTransform(String storeName){
        this.storeName = storeName;
    }
    @Override
    public void init(ProcessorContext context) {
        this.context = context;
        this.kvStore = new LinkedList<>();
        this.record = new ArrayList<>();
    }

    public KeyValue<String, String> transform(String key, String value) {
        if (commitTime ==0){
            commitTime = System.currentTimeMillis();
        }

        if ((System.currentTimeMillis()-commitTime)>= commitInvertal){
            commitTime=0;
            context.commit();
        }

        if (startTime == 0){
            startTime = System.currentTimeMillis();
        }
        if ((System.currentTimeMillis()-startTime)>= window_size){
            startTime =0;
            if (isfinished){
//                return null;
            }else {
                Weight = weightComputing();
                Counter = kvStore.size();
                sendToTopic(false);
                win_index++;
//                return null;
            }
            System.out.println(storeName+" a window number of data  "+oneWindowNum+ " win number "+ win_index);
            oneWindowNum=0;
        }

        if (!value.contains("C:") && !value.contains("W:") && !value.equals("last")){
            total++;
            oneWindowNum++;
            chunk_num++;
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
            System.out.println(storeName+" total number  "+total);
            context.commit();
            if(chunk_num>0){
                Weight = weightComputing();
                System.out.println("last");
                Counter =kvStore.size();
            }
            sendToTopic(true);
            win_index++;

        }


        return null;
    }

    @Override
    public KeyValue<String, String> punctuate(long timestamp) {
        return null;
    }

    @Override
    public void close() {
        // Any code for clean up would go here.  This processor instance will not be used again after this call.
    }


    private double weightComputing(){
        double update_carried_wt =currentWeight;
        if (currentCounter>1 && chunk_num> sampleSize){
            update_carried_wt = update_carried_wt*(currentCounter/(double)chunk_num);
        }
        double window_weight = 1.0;
        if (chunk_num>sampleSize){
            window_weight = chunk_num/(double) sampleSize;
        }
        double wt = window_weight*update_carried_wt;
        return  wt;
    }
    void Sampling(String value){
        if(kvStore.size()<=sampleSize){
            kvStore.add(value);
        }else {
            int position = getRandomNumberInRange(0, sampleSize-1);
            kvStore.set(position,value);
        }
    }

    int getRandomNumberInRange(int min, int max) {

        if (min >= max) {
            throw new IllegalArgumentException("max must be greater than min");
        }

        Random r = new Random();
        return r.nextInt((max - min) + 1) + min;
    }

     void sendToTopic(boolean isFinal){
            context.forward("window", String.valueOf(win_index));
           context.forward("weight", String.valueOf(Weight));
           context.forward("counter", String.valueOf(Counter));
         for (String item : kvStore){
             context.forward("data", item);

         }
        if(isFinal){
            context.forward("last", "last");
            isfinished = true;
        }
         context.commit();
         Weight = 1.0;
         Counter = 0;
         chunk_num = 0;
         kvStore.clear();
    }
}
