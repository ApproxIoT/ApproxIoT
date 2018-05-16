package io.confluent.examples.consumer;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.Collections;
/**
 * Created by zhenyuwen on 30/08/2017.
 */
public class shareBetween {
    ConcurrentHashMap<Integer, Integer> stream_num;
    int max_stream;
    int totalStream;
    int hasFinished = 0;

    public shareBetween(int max){
        this.stream_num = new ConcurrentHashMap<>();
        this.max_stream = max;
        this.totalStream =max;
        stream_num.put(0,max_stream);
    }

    public void changeStreamNum(int windNum){
        hasFinished++;
        if (stream_num.containsKey(windNum+1)){
            int num = stream_num.get(windNum+1);
            num --;
            stream_num.replace(windNum+1,num);
            updateTheMap(windNum+1);
        }else {
            Set<Integer> Keyset = stream_num.keySet();
            List<Integer> allKeys = new ArrayList<>(Keyset);
            Collections.sort(allKeys);
            int position=0;
            for (int key: allKeys){
                if (key<=windNum){
                    position=key;
                }else {
                    break;
                }
            }
            System.out.println("the position "+ position);
            int num = stream_num.get(position);
            System.out.println("windnum "+windNum+ " num " + num);
            num --;
            stream_num.put(windNum+1,num);
            updateTheMap(windNum+1);
        }

    }

    void updateTheMap(int window){
        Set<Integer> Keyset = stream_num.keySet();
        List<Integer> allKeys = new ArrayList<>(Keyset);
        Collections.sort(allKeys);
//        int position =0;
        for (int key: allKeys){
            if (key>window){
                int num = stream_num.get(key);
                num --;
                stream_num.replace(key,num);
            }
        }
    }

    public int getMax_stream(int winNum){
        if (stream_num.containsKey(winNum)){
            return stream_num.get(winNum);
        }else {
            Set<Integer> Keyset = stream_num.keySet();
            List<Integer> allKeys = new ArrayList<>(Keyset);
            Collections.sort(allKeys);
            int position=0;
            for (int key: allKeys){
                if (key<winNum){
                    position=key;
                }else {
                    break;
                }
            }
            return stream_num.get(position);
        }
    }

    public void printkeyStore(){
        Set<Integer> Keyset = stream_num.keySet();
        List<Integer> allKeys = new ArrayList<>(Keyset);
        Collections.sort(allKeys);
        for (int key: allKeys){
            System.out.println(key+" "+ stream_num.get(key));
        }
    }

    public boolean isAllStreamHasFinished(){
        if(totalStream==hasFinished){
            return true;
        }else {
            return false;
        }
    }
}
