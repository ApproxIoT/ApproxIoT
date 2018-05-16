package io.confluent.examples.producer;


import org.apache.kafka.clients.producer.ProducerConfig;


import java.util.Properties;

/**
 * Created by zhenyuwen on 01/08/2017.
 */
public class CreateInputStreams {

    // add kafka server ip address
    String ip_server = " ";


    public void writeToKafka( String[] subStreams) throws Exception{
        final Properties props = new Properties();
        props.put(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, 15728640);
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, ip_server);
        props.put("key.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");
//        props.put(ProducerConfig.LINGER_MS_CONFIG, 100);
//        props.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaUrl);

        for (String streamName : subStreams){
            Thread forward = new Thread(new writeToKafka(streamName,props));
            forward.start();
        }

    }

    public static void main(String[] args) throws Exception{
          String[] subStreams = {"S1", "S2", "S3", "S4"};


        CreateInputStreams cis = new CreateInputStreams();
        cis. writeToKafka(subStreams);

    }

}
