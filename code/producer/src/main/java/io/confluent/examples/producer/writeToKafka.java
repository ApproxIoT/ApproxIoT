package io.confluent.examples.producer;

import com.opencsv.CSVReader;
import org.apache.commons.math3.distribution.NormalDistribution;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import com.opencsv.CSVWriter;

import static org.apache.commons.math3.distribution.NormalDistribution.DEFAULT_INVERSE_ABSOLUTE_ACCURACY;

/**
 * Created by zhenyuwen on 07/08/2017.
 */
public class writeToKafka implements Runnable{
    private String streamName;
    private Properties props;
    private int total = 0;
    int data_item= 500000;
    String path = "/xx/xxxx/code/";
    writeToKafka(String streamName, Properties props){
        this.streamName = streamName;
        this.props = props;

    }
    public void run() {

        String downStream = streamName+"_WANRSlevel0";
        final KafkaProducer<String, String> producer = new KafkaProducer<>(props);
        try{

            System.out.println("read data from "+path+streamName+".csv");
            CSVReader reader = new CSVReader(new FileReader(path+streamName+".csv"));
            System.out.println("star sending data to "+downStream);
            List<String[]> allRows = reader.readAll();
            int i =0;
            for(String[] row : allRows){
                if (i>0){
                    StringBuilder strBuilder = new StringBuilder();
                    strBuilder.append(row[0]+",");
                    strBuilder.append(row[1]+",");
                    strBuilder.append(row[2]+",");
                    strBuilder.append(row[3]);
                    String newString = strBuilder.toString();
                    producer.send(new ProducerRecord<>(downStream, "data", newString));

                }

                i++;
            }
        }catch (IOException e){
            System.out.println(e);
        }
        producer.send(new ProducerRecord<>(downStream, "data", "last"));

        System.out.println("Message sent successfully "+ downStream);
        producer.flush();
        producer.close();
    }

    void generateTestData() {
        NormalDistribution generator = new NormalDistribution(100, 10, 5);
        ArrayList<String[]> record = new ArrayList<>();
        for (int i = 0; i < data_item; i++) {
            double data = generator.sample();
            String[] item = new String[2];
            item[0] = String.valueOf(data);
            record.add(item);
            total++;
        }
        try {

            CSVWriter writer = new CSVWriter(new FileWriter(path + streamName + ".csv"));
            writer.writeAll(record);
            writer.close();
            System.out.println("test data generated");
        } catch (IOException e) {
            System.out.println(e);
        }

        try{
            CSVWriter writer = new CSVWriter(new FileWriter(path+streamName+"num.csv"));
            String [] item = new String[1];
            item[0] = String.valueOf(total);
            writer.writeNext(item);
            writer.close();

        }catch (IOException e){
            System.out.println(e);
        }

    }
}
