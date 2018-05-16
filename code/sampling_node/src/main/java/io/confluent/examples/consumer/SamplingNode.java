package io.confluent.examples.consumer;

import kafka.admin.AdminUtils;
import org.I0Itec.zkclient.ZkConnection;
import kafka.admin.RackAwareMode;
import kafka.utils.ZKStringSerializer$;
import kafka.utils.ZkUtils;
import org.I0Itec.zkclient.ZkClient;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.processor.StateStoreSupplier;
import org.apache.kafka.streams.processor.TopologyBuilder;
import org.apache.kafka.streams.processor.WallclockTimestampExtractor;

import java.util.Properties;

/**
 * Created by zhenyuwen on 08/08/2017.
 */
public class SamplingNode {

    // add kafka server ip address
    String ip = " ";

    StringSerializer stringSerializer = new StringSerializer();
    StringDeserializer stringDeserializer = new StringDeserializer();
    public void runStream(){
        String [] subStream = new String[4];
        String node = "node";
        subStream= new String[]{"S1", "S2", "S3", "S4","S5", "S6", "S7", "S8"};
        node = "node1";

        Properties config = new Properties();

        config.put(StreamsConfig.APPLICATION_ID_CONFIG,
                "ApproxIoT_WAN_TP_First_"+node);
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,
                ip);
        config.put(ConsumerConfig.FETCH_MAX_BYTES_CONFIG, 1024 * 1024 * 40);
        config.put(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, 1024 * 1024 * 40);
        config.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, 1);
        config.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, WallclockTimestampExtractor.class);
        TopologyBuilder topologyBuilder = new TopologyBuilder();

        for (String name : subStream){
//            System.out.println(name);
            createToplogyBulider(name,topologyBuilder);
        }
        KafkaStreams streams = new KafkaStreams(topologyBuilder, config);
        streams.start();
    }


    void createToplogyBulider(String name, TopologyBuilder topologyBuilder){
        String theTopic = name+"_WANRSlevel0";
        String downStream = name+"_WANRSlevel1";
        System.out.println(theTopic);
        topologyBuilder.addSource("source_"+name, stringDeserializer, stringDeserializer, theTopic)
                .addProcessor("sampling_"+name,()->new ResoviorSamplingProcessor(name), "source_"+name)
                .addSink(name+"_sink", downStream, stringSerializer, stringSerializer, "sampling_"+name);
    }

    public static void main(String[] args) throws Exception{
        SamplingNode start = new SamplingNode();
        start.runStream();
    }
}
