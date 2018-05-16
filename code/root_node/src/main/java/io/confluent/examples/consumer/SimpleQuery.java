package io.confluent.examples.consumer;

import com.opencsv.CSVWriter;

import java.io.FileWriter;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by zhenyuwen on 14/09/2017.
 */
public class SimpleQuery implements Runnable {

    private ConcurrentHashMap<Integer, SRSSwindow> block;
    volatile shareBetween share;
    int time =0;

    public SimpleQuery(ConcurrentHashMap<Integer, SRSSwindow> block, shareBetween share){
        this.block = block;
        this.share = share;
    }

    public void run() {
        try {
            System.out.println
                    ("Start " + Thread.currentThread().getName());
            System.out.println
                    ("Start query thread for no sampling" );
            while (true) {
                while (!block.isEmpty()) {
                    Set<Integer> Keyset = block.keySet();
                    List<Integer> allKeys = new ArrayList<>(Keyset);
                    Collections.sort(allKeys);
                    for (int key : allKeys) {
                        SRSSwindow wd = block.get(key);
                        if (wd.getFinishMarks() == share.getMax_stream(key)) {
                            System.out.println(key+" has finished "+wd.getFinishMarks());
                            share.printkeyStore();
                            if (wd.isHasData()) {
                                double sum = 0;
                                System.out.println("execution_time "+ time);
                                time++;
                                HashMap<String, ArrayList<Double>> data = wd.getData();
                                for (String sub_Stream: data.keySet()){
                                    ArrayList<Double> list = data.get(sub_Stream);
                                    if (!list.isEmpty()){
                                        sum += sum(list);
                                    }
                                }
                                System.out.println("the sum of window "+key+" "+sum);

                            }
                            System.out.println("remove the key "+key);
                            block.remove(key);
                            if(share.isAllStreamHasFinished()){
                                System.out.println("stop query");
                                System.out.println("queue size "+ block.size());
                                if (block.size()>0){
                                    Set<Integer> theKeys = block.keySet();
                                    for (int k : theKeys) {
                                        System.out.println("left key "+ k);
                                        SRSSwindow sswd = block.get(k);
                                        System.out.println("has finihsed "+ sswd.getFinishMarks());
                                        System.out.println("required finihsed "+ share.getMax_stream(k));

                                    }
                                }
                                if(block.isEmpty()){
                                    Thread.sleep(5000);
                                    System.exit(0);
                                }
                            }
                        }
                    }
                }
            }
        }catch (Exception e) {
            System.out.println
                    (Thread.currentThread().getName() + " " + e.getMessage());
            System.exit(0);
        }
    }

    double sum(ArrayList<Double> list) {
        return list.stream().mapToDouble(Double::doubleValue).sum();
    }

}
