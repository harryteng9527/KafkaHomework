package org.example;
import org.apache.kafka.clients.producer.*;

import java.util.Properties;

public class Main {
    public static void main(String[] args) {
        int argsSize = args.length;
        int i = 0;
        String topicName = null;
        int records, recordSize;
        Properties props = new Properties();

        for(i = 0 ; i < argsSize ; i++){
            switch (args[i]) {
                case "--brokers":
                    props.put("bootstrap.servers", args[++i]);
                    break;
                case "--topic":
                    topicName = args[++i];
                    break;
                case "--records":
                    records = Integer.parseInt(args[++i]);
                    break;
                case "--recordSize":
                    recordSize = Integer.parseInt(args[++i]);
                    break;
            }
        }

        props.put("acks", "1");
        props.put("retries", 1);
        props.put("linger.ms", 10);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        Producer<String, String> producer = new KafkaProducer<>(props);
        for (i = 0; i < 10000; i++)
            producer.send(new ProducerRecord<String, String>(topicName, Integer.toString(i), Integer.toString(i)));

        producer.close();

        System.out.println("walwala");
    }
}
