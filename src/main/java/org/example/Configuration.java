package org.example;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;

import java.util.Properties;

public class Configuration {
    public void config(Properties prop){
        prop.put(ProducerConfig.ACKS_CONFIG,"1");
        prop.put(ProducerConfig.LINGER_MS_CONFIG,10);
        prop.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer");
        prop.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer");
    }
}
