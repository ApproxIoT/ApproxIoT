package io.confluent.examples.producer;

/**
 * Created by zhenyuwen on 29/09/2017.
 */

import kafka.admin.AdminUtils;
import kafka.utils.ZKStringSerializer$;
import kafka.utils.ZkUtils;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkConnection;
import org.apache.kafka.common.requests.MetadataResponse;

import java.io.IOException;

public class DeleteTopics {

    // add kafka server ip address
    String ip = " ";



    void delete(String name){
        String zookeeperConnect = ip;
        String thirdLayerTopic = name+"_WANRSlevel2";
        int sessionTimeoutMs = 10 * 1000;
        int connectionTimeoutMs = 8 * 1000;
        ZkClient zkClient = new ZkClient(
                zookeeperConnect,
                sessionTimeoutMs,
                connectionTimeoutMs,
                ZKStringSerializer$.MODULE$);
        boolean isSecureKafkaCluster = false;
        ZkUtils zkUtils = new ZkUtils(zkClient, new ZkConnection(zookeeperConnect), isSecureKafkaCluster);



        if (AdminUtils.topicExists(zkUtils, thirdLayerTopic)) {
            AdminUtils.deleteTopic(zkUtils, thirdLayerTopic);
            System.out.println(thirdLayerTopic);
        }

    }

    void deletelevel1(String name){
        String zookeeperConnect = ip;
        String firstLayerTopic = name+"_WANRSlevel0";
        int sessionTimeoutMs = 10 * 1000;
        int connectionTimeoutMs = 8 * 1000;
        ZkClient zkClient = new ZkClient(
                zookeeperConnect,
                sessionTimeoutMs,
                connectionTimeoutMs,
                ZKStringSerializer$.MODULE$);
        boolean isSecureKafkaCluster = false;
        ZkUtils zkUtils = new ZkUtils(zkClient, new ZkConnection(zookeeperConnect), isSecureKafkaCluster);
        if (AdminUtils.topicExists(zkUtils, firstLayerTopic)) {
            System.out.println(firstLayerTopic);

            AdminUtils.deleteTopic(zkUtils, firstLayerTopic);

        }
    }

    void deletelevel2(String name){
        String zookeeperConnect = ip;
        String secondLayerTopic = name+"_WANRSlevel1";
        int sessionTimeoutMs = 10 * 1000;
        int connectionTimeoutMs = 8 * 1000;
        ZkClient zkClient = new ZkClient(
                zookeeperConnect,
                sessionTimeoutMs,
                connectionTimeoutMs,
                ZKStringSerializer$.MODULE$);
        boolean isSecureKafkaCluster = false;
        ZkUtils zkUtils = new ZkUtils(zkClient, new ZkConnection(zookeeperConnect), isSecureKafkaCluster);
        if (AdminUtils.topicExists(zkUtils, secondLayerTopic)) {
            AdminUtils.deleteTopic(zkUtils, secondLayerTopic);
            System.out.println(secondLayerTopic);
        }
    }
    public static void main(String[] args) throws Exception{
        int Num = 32;
        String [] subStreams = new String[Num];
        for(int a =0; a<Num; a++){

            subStreams[a]= "S"+String.valueOf(a+1);

        }
        DeleteTopics dt = new DeleteTopics();

        for (String streamName : subStreams){
           dt.delete(streamName);
            Thread.sleep(1000);
            dt.deletelevel1(streamName);
            Thread.sleep(1000);
            dt.deletelevel2(streamName);
            Thread.sleep(1000);
        }

    }

}
