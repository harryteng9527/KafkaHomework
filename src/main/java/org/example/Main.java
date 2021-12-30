package org.example;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.header.Header;

import java.util.List;
import java.util.Properties;

public class Main {
    public static void main(String[] args) {
        int i = 0;
        String key = null, value = null;

        Properties props = new Properties();
        Argument argument = Argument.parse(args);

        Header data = new Header() {
            @Override
            public String key() {
                return "";
            }

            @Override
            public byte[] value() {
                return new byte[argument.recordSize];
            }
        };
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,argument.bootstrapServer);

        Configuration.config(props);

        Producer<String, String> producer = new KafkaProducer<>(props);
        for (i = 0; i < argument.records; i++) {
            key = String.format("key-%06d", i);
            value = String.format("value-%06d",i);
            producer.send(new ProducerRecord<String, String>(argument.topicName,
                    null,
                    key,
                    value,
                    List.of(data)));
        }
        producer.close();

    }
}
