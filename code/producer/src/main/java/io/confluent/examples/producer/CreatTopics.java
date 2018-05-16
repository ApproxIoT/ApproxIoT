package io.confluent.examples.producer;

import kafka.admin.AdminUtils;
import kafka.admin.RackAwareMode;
import kafka.utils.ZKStringSerializer$;
import kafka.utils.ZkUtils;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkConnection;

import java.util.Properties;

/**
 * Created by zhenyuwen on 29/09/2017.
 */
public class CreatTopics {

    // add kafka server ip address
    String ip = " ";

    public  void createTopics(String streamName) {
        String downStream = streamName+"_WANRSlevel0";
        String zookeeperConnect = ip;
        int sessionTimeoutMs = 10 * 1000;
        int connectionTimeoutMs = 8 * 1000;
        ZkClient zkClient = new ZkClient(
                zookeeperConnect,
                sessionTimeoutMs,
                connectionTimeoutMs,
                ZKStringSerializer$.MODULE$);
        boolean isSecureKafkaCluster = false;
        ZkUtils zkUtils = new ZkUtils(zkClient, new ZkConnection(zookeeperConnect), isSecureKafkaCluster);
        if (AdminUtils.topicExists(zkUtils, downStream)){
            System.out.println(downStream+" Topic already exist");
        }else {
            int partitions = 7;
            int replication = 2;
            Properties topicConfig = new Properties();

            AdminUtils.createTopic(zkUtils, downStream, partitions, replication, topicConfig, RackAwareMode.Enforced$.MODULE$);
            zkClient.close();
            System.out.println(downStream+" Topic has been created");
        }
    }


    void creatSampleNodes(String streamName ){
        String downStream = streamName+"_WANRSlevel1";
        String zookeeperConnect = ip;
        int sessionTimeoutMs = 10 * 1000;
        int connectionTimeoutMs = 8 * 1000;
        ZkClient zkClient = new ZkClient(
                zookeeperConnect,
                sessionTimeoutMs,
                connectionTimeoutMs,
                ZKStringSerializer$.MODULE$);
        boolean isSecureKafkaCluster = false;
        ZkUtils zkUtils = new ZkUtils(zkClient, new ZkConnection(zookeeperConnect), isSecureKafkaCluster);
        if (AdminUtils.topicExists(zkUtils, downStream)){
            System.out.println(downStream+" Topic already exist");
        }else {
            int partitions = 7;
            int replication = 2;
            Properties topicConfig = new Properties();

            AdminUtils.createTopic(zkUtils, downStream, partitions, replication, topicConfig, RackAwareMode.Enforced$.MODULE$);
            zkClient.close();
            System.out.println(downStream+" Topic has been created");
        }
    }

    void creatSampleNodesSecond(String streamName ){
        String downStream = streamName+"_WANRSlevel2";
        String zookeeperConnect = ip;
        int sessionTimeoutMs = 10 * 1000;
        int connectionTimeoutMs = 8 * 1000;
        ZkClient zkClient = new ZkClient(
                zookeeperConnect,
                sessionTimeoutMs,
                connectionTimeoutMs,
                ZKStringSerializer$.MODULE$);
        boolean isSecureKafkaCluster = false;
        ZkUtils zkUtils = new ZkUtils(zkClient, new ZkConnection(zookeeperConnect), isSecureKafkaCluster);
        if (AdminUtils.topicExists(zkUtils, downStream)){
            System.out.println(downStream+" Topic already exist");
        }else {
            int partitions = 7;
            int replication = 2;
            Properties topicConfig = new Properties();

            AdminUtils.createTopic(zkUtils, downStream, partitions, replication, topicConfig, RackAwareMode.Enforced$.MODULE$);
            zkClient.close();
            System.out.println(downStream+" Topic has been created");
        }
    }


    public static void main(String[] args) throws Exception{
        int Num = 31;
        String [] subStreams = new String[Num];
        for(int a =0; a<Num; a++){

            subStreams[a]= "S"+String.valueOf(a+1);

        }

        CreatTopics ct = new CreatTopics();

        for (String streamName : subStreams){
            ct.createTopics(streamName);
            Thread.sleep(1000);
            ct.creatSampleNodes(streamName);
            Thread.sleep(1000);
            ct.creatSampleNodesSecond(streamName);
            Thread.sleep(1000);
        }

    }
}
