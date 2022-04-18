package consumer;

import org.apache.kafka.clients.consumer.internals.AbstractPartitionAssignor;
import org.apache.kafka.common.TopicPartition;

import javax.swing.text.html.HTMLDocument;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;

public class RandomAssignor extends AbstractPartitionAssignor {
    @Override
    public String name(){
        return "random";
    }

    @Override
    public Map<String, List<TopicPartition>> assign(
            Map<String,Integer> partitionsPerTopic,
            Map<String, Subscription> subscriptions
    ){
        //
        Map<String, List<String>> consumersPerTopic = consumersPerTopic(subscriptions);
        Map<String, List<TopicPartition>> assignment = new HashMap<>();
        for(String memberID : subscriptions.keySet()){
            assignment.put(memberID, new ArrayList<>());
        }

        for(Map.Entry<String, List<String>> topicEntry : consumersPerTopic.entrySet()){
            String topic = topicEntry.getKey();
            List<String> consumersForTopic = topicEntry.getValue();
            int consumerSize = consumersForTopic.size();

            Integer numPartitionsForTopic = partitionsPerTopic.get(topic);
            if(numPartitionsForTopic == null){
                continue;
            }

            List<TopicPartition> partitions = AbstractPartitionAssignor.partitions(topic, numPartitionsForTopic);

            for(TopicPartition partition : partitions){
                int rand = new Random().nextInt(consumerSize);
                String randomConsumer = consumersForTopic.get(rand);
                assignment.get(randomConsumer).add(partition);
            }
        }

        for(Map.Entry<String, Subscription> subscriptionEntry : subscriptions.entrySet()){
            System.out.println("In assign method !");
            System.out.println("subscription get value : " + subscriptionEntry.getValue());
            System.out.println("owned Partitions : " + subscriptionEntry.getValue().ownedPartitions());
            System.out.println("userData : " + subscriptionEntry.getValue().userData());
            System.out.println("assignment key: " + assignment.keySet());
            System.out.println("assignment value: " + assignment.values());
            System.out.println("=======================================================");
        }

        assignment.forEach((k,v)->
                v.forEach
                (topicPartition ->
                        System.out.println("Key : " +k
                                +", topic: "+topicPartition.topic()
                                +", partition: "+topicPartition.partition())));

        System.out.println("==============================================");
        System.out.println("partitionPerTopic");
        partitionsPerTopic.forEach((k,v) ->
                System.out.println("key = " + k + " , value = " + v));
        System.out.println("==============================================");

        return assignment;
    }

    private Map<String, List<String>> consumersPerTopic(
            Map<String, Subscription> consumerMetadata
    ){
        Map<String, List<String>> result = new HashMap<>();
        //
        for(Map.Entry<String, Subscription> subscriptionEntry : consumerMetadata.entrySet()){
            String consumerID = subscriptionEntry.getKey();
            for(String topic : subscriptionEntry.getValue().topics())
                put(result, topic, consumerID);
        }

        for(Map.Entry<String, Subscription> subscriptionEntry : consumerMetadata.entrySet()){
            System.out.println("subscription get value : " + subscriptionEntry.getValue());
            System.out.println("owned Partitions : " + subscriptionEntry.getValue().ownedPartitions());
            System.out.println("userData : " + subscriptionEntry.getValue().userData());
            System.out.println("get.value.topic" + subscriptionEntry.getValue().topics());
            System.out.println("=======================================================");
        }


        result.forEach((key, value) ->
                value.forEach( Subscription ->
                        System.out.println("There is result , key = " + key
                                + "value = " + value
                                + "get byte" + Subscription.getBytes() )));

        return result;
    }
}
