package org.example;
import org.apache.kafka.clients.producer.*;

import java.util.ListIterator;
import java.util.Properties;

import static java.lang.String.format;

public class Main {
    public static void main(String[] args) {
        int argsSize = args.length;
        int i = 0;
        String topicName = null;
        String key = null, value = null;
        int records=0, recordSize=0;
        byte[] empty= new byte[980];
        String em = new String(empty);

        Properties props = new Properties();

        for(i = 0 ; i < argsSize ; i++){
            switch (args[i]) {
                case "--brokers":
                    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, args[++i]);
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

        Configuration conf = new Configuration();
        conf.config(props);


        Producer<String, String> producer = new KafkaProducer<>(props);
        for (i = 0; i < records; i++) {
            key = String.format("key-%06d", i);
            value = String.format("value-%06d",i);
            value = value + em;
            producer.send(new ProducerRecord<String, String>(topicName,key, value));
        }
        producer.close();

        System.out.println("walawalaa");
    }
}
