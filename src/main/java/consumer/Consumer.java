package consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.example.Configuration;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Properties;

public class Consumer {

    public static void main(String[] args) {
        Properties props = new Properties();

        ParseArg parseArg = ParseArg.parseArg(args);

        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,parseArg.bootstrapServer);
        props.put(ConsumerConfig.CLIENT_ID_CONFIG,parseArg.clientID);
        props.put(ConsumerConfig.GROUP_ID_CONFIG,parseArg.groupID);

        /*
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.103.24:13647");
        props.put(ConsumerConfig.CLIENT_ID_CONFIG,"007");
        props.put(ConsumerConfig.GROUP_ID_CONFIG,"dokiii");
        */
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class.getName());
        props.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG,RandomAssignor.class.getName());

        Configuration.config(props);

        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);
/*
        System.out.println("I'm "+ parseArg.clientID
                + ", my member id is " + consumer.groupMetadata().memberId()
        + ", generation ID is " +consumer.groupMetadata().generationId()
        + ", groupInstanceID is " + consumer.groupMetadata().groupInstanceId());
*/

        System.out.println("I'm " + parseArg.clientID
                + ", my member id is " + consumer.groupMetadata().memberId()
        + ", generation ID is " +consumer.groupMetadata().generationId()
        + ", groupInstanceID is " + consumer.groupMetadata().groupInstanceId());

        String[] array = {"test1", "test2", "test3"};

        consumer.subscribe(Arrays.asList(array));

        System.out.println(consumer.subscription());
        System.out.println("------------------------------------------------");

        System.out.println(consumer.partitionsFor("test1"));
        System.out.println(consumer.partitionsFor("test2"));
        System.out.println(consumer.partitionsFor("test3"));


        while (true){
            ConsumerRecords<String,String> records = consumer.poll(Duration.ofMillis(100));
            for(ConsumerRecord<String,String> record : records){
                //System.out.println("Key: "+record.key() + ", Value: "+record.value());
                //System.out.println("Topic: " + record.topic()
                  //      + ", Partition: " + record.partition()
                    //    + ", Offset: "+record.offset());
            }
        }

    }
}
