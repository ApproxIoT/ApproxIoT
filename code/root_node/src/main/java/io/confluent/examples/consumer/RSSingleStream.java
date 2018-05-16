package io.confluent.examples.consumer;

import org.apache.kafka.streams.kstream.KStream;

import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Created by zhenyuwen on 10/10/2017.
 */
public class RSSingleStream implements Runnable{
    private String name;
    private ConcurrentHashMap<Integer, window> block;
    private boolean isFirst = true;
    private int win_counter=-1;
    int temp_window;
    double weight=1.0;
    int counter=0;
    volatile shareBetween share;
    ReentrantLock lock;
    private KStream<String, String> stream;
    ArrayList<Double> sample = new ArrayList<>();

    public RSSingleStream(KStream<String, String> stream,String name,
                          ConcurrentHashMap<Integer, window> block,
                          shareBetween share,
                          ReentrantLock lock){
        this.stream = stream;
        this.name = name;
        this.block = block;
        this.share = share;
        this.lock = lock;
    }

    public void run() {
        try{
//            System.out.println
//                    ("Start receiving data from "+ name );
            KStream<String, String> transformed = stream.transform(()->new ResoviorSamplingTransform(name));
            transformed.foreach((key, value) -> {
                if(key.equals("window")){
                    if (isFirst){
                        temp_window = Integer.valueOf(value);
                        win_counter = temp_window;
                        isFirst=false;
                    }else {
                        temp_window = Integer.valueOf(value);
                        if (temp_window==win_counter){
                            PushToMap(weight,counter,sample,false);
                        }else {
                            PushToMap(weight,counter,sample,true);
                            win_counter = temp_window;
                        }

                        weight=1.0;
                        counter=0;
                        sample.clear();
                    }
                }
                if(key.equals("weight")){
                    weight = Double.valueOf(value);
                }

                if (key.equals("counter")){
                    counter = Integer.valueOf(value);
                }
                if (key.equals("data")){
                    sample.add(Double.valueOf(value));
                }
                if (key.equals("last")){

//                    System.out.println("last batch size "+ name+" window "+ win_counter);
                    share.changeStreamNum(win_counter);
                    share.printkeyStore();
//                if (!sample.isEmpty()){
                    win_counter = temp_window;
                    PushToMap(weight,counter,sample,true);
//                }
                }
            });

        }catch (Exception e) {
            System.out.println
                    (Thread.currentThread().getName() + " " + e.getMessage());
            System.exit(0);
        }
    }

    public void PushToMap(double weight, int counter, ArrayList<Double> sample, boolean IsEndofWindow){
//        System.out.println("queue size "+ block.size());
        lock.lock();
//        System.out.println(name +" windows "+win_counter );
        if(block.containsKey(win_counter)){
            window wd = block.get(win_counter);
            wd.setWeight(name, weight);
            wd.setCounter(name,counter);
            wd.setData(name, (ArrayList<Double>)sample.clone());
            if(IsEndofWindow){
                wd.markFinish();
//                System.out.println(name +" marked" );
            }
            block.replace(win_counter,wd);
        }else {
            window wd = new window(win_counter);
            wd.setWeight(name, weight);
            wd.setCounter(name,counter);
            wd.setData(name, (ArrayList<Double>)sample.clone());
            if(IsEndofWindow){
                wd.markFinish();
//                System.out.println(name +" marked" );
            }
            block.put(win_counter,wd);
        }
        lock.unlock();
    }
}
