package io.confluent.examples.consumer;

import java.util.ArrayList;
import java.util.HashMap;

/**
 * Created by zhenyuwen on 08/08/2017.
 */
public class window {
    private int window_id;
    private HashMap<String, ArrayList<Double>> weight;
    private HashMap<String, ArrayList<ArrayList<Double>>> data;
    private HashMap<String, ArrayList<Integer>> counter;
    private boolean hasData =false;
    private int hasFinish =0;

    public window(int window_id ){
        this.window_id = window_id;
        this.weight = new HashMap<>();
        this.data = new HashMap<>();
        this.counter = new HashMap<>();
    }

    public void setWeight(String name,  double wt){
        if(weight.containsKey(name)){
            ArrayList<Double> temp = weight.get(name);
            temp.add(wt);
            weight.put(name,temp);
        }else {
            ArrayList<Double> temp = new ArrayList<>();
            temp.add(wt);
            weight.put(name,temp);
        }
    }

    public void setCounter(String name, int ct){

        if(counter.containsKey(name)){
            ArrayList<Integer> temp = counter.get(name);
            temp.add(ct);
            counter.put(name,temp);
        }else {
            ArrayList<Integer> temp = new ArrayList<>();
            temp.add(ct);
            counter.put(name,temp);
        }
    }

    public void setData(String name, ArrayList<Double> sample){
        if (!data.isEmpty()){
            if(data.containsKey(name)){
                ArrayList<ArrayList<Double>> samples = data.get(name);
                samples.add(sample);
                data.put(name,samples);
            }else {
                ArrayList<ArrayList<Double>> samples = new ArrayList<>();
                samples.add(sample);
                data.put(name,samples);
            }
        }else {
            ArrayList<ArrayList<Double>> samples = new ArrayList<>();
            samples.add(sample);
            data.put(name,samples);
            hasData =true;
        }
    }

    public  void markFinish(){
        hasFinish++;
    }

    public int getWindow_id(){
        return window_id;
    }

    public HashMap<String, ArrayList<ArrayList<Double>>> getData(){
        return data;
    }

    public HashMap<String, ArrayList<Double>> getWeight(){
        return weight;
    }

    public HashMap<String, ArrayList<Integer>> getCounter(){
        return counter;
    }


    public int getFinishMarks(){
        return hasFinish;
    }

    public boolean isHasData(){
        return hasData;
    }
//    public boolean isFull(){
//        if (weight.size() == totalNum){
//            return true;
//        }else {
//            return false;
//        }
//    }
}
