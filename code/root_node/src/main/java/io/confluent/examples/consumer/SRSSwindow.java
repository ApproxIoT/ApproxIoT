package io.confluent.examples.consumer;

import java.util.ArrayList;
import java.util.HashMap;

/**
 * Created by zhenyuwen on 12/09/2017.
 */
public class SRSSwindow {
    private int window_id;
    private HashMap<String, ArrayList<Double>> data;
    private boolean hasData =false;
    private int hasFinish =0;
    public SRSSwindow(int window_id){
        this.window_id = window_id;
        this.data = new HashMap<>();
    }


    public void setData(String name, ArrayList<Double> sample){
        data.put(name,sample);
        if(!hasData) {
            if (!sample.isEmpty()) {
                hasData = true;
            }
        }
    }

    public HashMap<String, ArrayList<Double>> getData(){
        return data;
    }

    public void markFinish(){
        hasFinish++;
    }
    public int getFinishMarks(){
        return hasFinish;
    }
    public boolean isHasData(){
        return hasData;
    }
}
