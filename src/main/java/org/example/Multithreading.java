package org.example;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;

import java.util.List;
import java.util.Properties;

public class Multithreading extends Thread{
    private Properties props;
    private Argument argument;
    private String key = null, value = null;
    Header data = new Header(){
        @Override
        public String key(){ return "";}

        @Override
        public byte[] value(){ return new byte[argument.recordSize];}
    };

    Multithreading(Properties props, Argument argument){
        this.props = props;
        this.argument = argument;
    }

    public void run(){
        Producer<String, String> producer = new KafkaProducer<>(props);
        for(int j = 0 ; j < 6 ; j++) {
            for (int i = 0; i < argument.records; i++) {
                key = String.format("key-%06d", i);
                value = String.format("value-%06d", i);
                producer.send(new ProducerRecord<String, String>(argument.topicName,
                        null,
                        key,
                        value,
                        List.of(data)));
            }
            producer.close();
        }
        System.out.println("Finish the producing!");
    }
}
