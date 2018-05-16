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
 * Created by zhenyuwen on 12/09/2017.
 */
public class SimpleRandomSamplingTransform implements Transformer<String, String, KeyValue<String, String>> {

    private ProcessorContext context;
    private LinkedList<String> kvStore;
    private  String storeName;
    private int window_size = 5000;
    private double sampleRate = 1;
    private boolean isFirstData = true;
    private String path = "/home/zhenyu/code/";
    private Random rand;
    private int win_index = 0;
    private long startTime =0;
    private boolean isfinished = false;
    private long commitTime=0;
    private int commitInvertal=1000;
    private int total=0;
    private int oneWindowNum =0;

    public SimpleRandomSamplingTransform(String storeName){
        this.storeName = storeName;
        this.rand = new Random();
    }
    @Override
    public void init(ProcessorContext context) {
        this.context = context;
        this.kvStore = new LinkedList<>();
    }

    @Override
    public KeyValue<String, String> punctuate(long timestamp) {
        return null;
    }
    @Override
    public void close() {
        // Any code for clean up would go here.  This processor instance will not be used again after this call.
    }

    public KeyValue<String, String> transform(String key, String value) {
//        System.out.println(value);
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
            }else {
                System.out.println(storeName+" one window number of data " + oneWindowNum);
                sendToTopic(false);
                win_index++;
                oneWindowNum=0;
            }
        }
        if (!value.equals("last")){
            if (isFirstData){
                try{
                    System.out.println(storeName+" write start time");
                    CSVWriter writer = new CSVWriter(new FileWriter(path+storeName+"start.csv"));
//                        CSVWriter writer = new CSVWriter(new FileWriter(storeName+"start.csv"));
                    String [] item = new String[1];
                    item[0] = String.valueOf(System.currentTimeMillis());
                    writer.writeNext(item);
                    writer.close();
                    isFirstData = false;
                }catch (IOException e){
                    System.out.println(e);
                }
            }
            total++;
            oneWindowNum++;
            Sampling(value);
        }

        if (value.equals("last")){
            System.out.println(storeName+" total number " + total);
            context.commit();
            sendToTopic(true);
        }

        return null;
    }

    void Sampling(String value){
        double coin = rand.nextDouble();
        if (coin <= sampleRate){
            kvStore.add(value);
        }
    }

    void sendToTopic(boolean isFinal){
        for (String item : kvStore){
//            System.out.println(item);
            context.forward("data", item);
        }

        if(isFinal){
            context.forward("last", "last");
            isfinished = true;
        }else {
            context.forward("window", String.valueOf(win_index));
        }
        kvStore.clear();
    }
}
