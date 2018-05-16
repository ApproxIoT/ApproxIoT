package io.confluent.examples.consumer;

import java.io.FileWriter;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import com.opencsv.CSVWriter;

/**
 * Created by zhenyuwen on 08/08/2017.
 */
public class Query implements Runnable {
    private ConcurrentHashMap<Integer, window> block;
    volatile shareBetween share;
    int time =0;

    public Query(ConcurrentHashMap<Integer, window> block,  shareBetween share) {
        this.block = block;
        this.share = share;
    }

    public void run() {
        try {
            System.out.println
                    ("Start " + Thread.currentThread().getName());
            System.out.println
                    ("Start query thread" );
            while (true) {
                while (!block.isEmpty()) {
                    Set<Integer> Keyset = block.keySet();
                    List<Integer> allKeys = new ArrayList<>(Keyset);
                    Collections.sort(allKeys);
                    for (int key : allKeys) {
                        window wd = block.get(key);
                        if (wd.getFinishMarks() == share.getMax_stream(key)) {
                            System.out.println(key+" has finished "+wd.getFinishMarks());

                            share.printkeyStore();
                            if (wd.isHasData()) {
                                double sum = 0;
                                HashMap<String, ArrayList<Double>> weights = wd.getWeight();

                                System.out.println("execution_time "+ time);
                                time++;

                                HashMap<String, ArrayList<ArrayList<Double>>> data = wd.getData();
                                for (String sub_Stream : weights.keySet()) {
                                    if (data.containsKey(sub_Stream)) {
                                        ArrayList<ArrayList<Double>> singleStreamData = data.get(sub_Stream);
                                        ArrayList<Double> singleStreamWeight = weights.get(sub_Stream);
                                        for (int i =0; i<singleStreamData.size();i++){
                                            ArrayList<Double> list = singleStreamData.get(i);
                                            if(!list.isEmpty()){
                                                sum += sum(singleStreamData.get(i)) * singleStreamWeight.get(i);
                                            }
                                        }
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
                                        window sswd = block.get(k);
                                        System.out.println("has finihsed "+ sswd.getFinishMarks());
                                        System.out.println("required finihsed "+ share.getMax_stream(k));
                                    }
                                }
                            }
                        }
                    }
                }
            }

        } catch (Exception e) {
            System.out.println
                    (Thread.currentThread().getName() + " " + e.getMessage());
            System.exit(0);
        }
    }

    double sum(ArrayList<Double> list) {
        return list.stream().mapToDouble(Double::doubleValue).sum();
    }
}
