package io.confluent.examples.consumer;

import org.apache.kafka.streams.kstream.KStream;

import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Created by zhenyuwen on 12/10/2017.
 */
public class SRSSingleStream implements Runnable {
    private String name;
    private ConcurrentHashMap<Integer, SRSSwindow> block;
    private int win_counter=0;
    volatile shareBetween share;
    ArrayList<Double> sample = new ArrayList<>();
    ReentrantLock lock;
    private KStream<String, String> stream;

    public SRSSingleStream(KStream<String, String> stream, String name,
                           ConcurrentHashMap<Integer, SRSSwindow> block,
                           shareBetween share,
                           ReentrantLock lock){
        this.stream = stream;
        this.name = name;
        this.block = block;
        this.share = share;
        this.lock = lock;
    }

    public void run() {
        KStream<String, String> transformed = stream.transform(()->new SimpleRandomSamplingTransform(name));
        transformed.foreach((key, value) -> {
            if(key.equals("window")){
                win_counter = Integer.valueOf(value);
//                System.out.println("sample size "+ sample.size());
                PushToMap(sample);
                sample.clear();
            }

            if (key.equals("data")){
                sample.add(Double.valueOf(value));
            }
            if (key.equals("last")){
                win_counter++;
                System.out.println(name+" window "+ win_counter);
                share.changeStreamNum(win_counter);
//                System.out.println("sample size "+ sample.size());
                PushToMap(sample);

            }
        });
    }

    void PushToMap( ArrayList<Double> sample){
//        System.out.println("queue size "+ block.size());
        lock.lock();
//        System.out.println(name +" windows "+win_counter );
        if(block.containsKey(win_counter)){
            SRSSwindow wd = block.get(win_counter);
            wd.setData(name, (ArrayList<Double>)sample.clone());
            wd.markFinish();
            block.replace(win_counter,wd);
        }else {
            SRSSwindow wd = new SRSSwindow(win_counter);
            wd.setData(name, (ArrayList<Double>)sample.clone());
            wd.markFinish();
            block.put(win_counter,wd);
        }
        lock.unlock();
    }

}
