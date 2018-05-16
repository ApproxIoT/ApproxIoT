package io.confluent.examples.consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.processor.WallclockTimestampExtractor;

import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Created by zhenyuwen on 31/08/2017.
 */
public class RootNodeMain {


    volatile ConcurrentHashMap<Integer, window> block;
    ReentrantLock lock;
    volatile shareBetween share;
    // add kafka server ip address
        String ip = " ";

    public void FetchSubStreams(){
        int Num = 31;
        String [] subStream = new String[Num];
        for(int a =0; a<Num; a++){
            subStream[a]= "S"+String.valueOf(a+1);
//            System.out.println(subStream[a]);
        }
        System.out.println("total streams " + subStream.length);
        block = new ConcurrentHashMap<>();
        lock = new ReentrantLock();
        share = new shareBetween(subStream.length);
        Properties config = new Properties();
//         application id must be different when operate the shared topics
        config.put(StreamsConfig.APPLICATION_ID_CONFIG,
                "ApproxIoT-WAN-TP-RS-ROOT");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,
                ip);
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        config.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 1);
        config.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, 1);
        config.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, WallclockTimestampExtractor.class);

        KStreamBuilder builder = new KStreamBuilder();
        for (String name : subStream){
            String topic = name+"_WANRSlevel0";
            System.out.println
                    ("Start receiving data from "+ topic);
            KStream<String, String> stream =builder.stream(
                    Serdes.String(), Serdes.String(),topic);
            Thread singleStream = new Thread(new RSSingleStream(stream, name, block, share, lock));
            singleStream.start();
        }
        Thread query = new Thread(new Query(block, share));
        query.start();
        KafkaStreams streams = new KafkaStreams(builder, config);
        streams.start();
    }

    public static void main(String[] args) throws Exception{
        RootNodeMain start =new RootNodeMain();
        start.FetchSubStreams();
    }
}
