import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.KafkaFuture;

import java.util.*;
import java.util.concurrent.ExecutionException;

public class AdminClientService {

    public AdminClient getAdminClient() {
        Properties kafkaProps = new Properties();
        kafkaProps.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.1.71:9092");
        return AdminClient.create(kafkaProps);
    }

    public void createTopic(String name, int numPartitions, int replicationFactor){
        NewTopic _newTopic = new NewTopic(name, numPartitions, (short) replicationFactor);

        List<NewTopic> topicList = new ArrayList<>();
        topicList.add(_newTopic);

        CreateTopicsResult createTopicsResult = getAdminClient().createTopics(topicList);
        for (Map.Entry<String, KafkaFuture<Void>> entry : createTopicsResult.values().entrySet()) {
            System.out.println("Topic("+entry.getKey()+") is created");
        }

    }

    public void deleteTopic(String topicName){
        List<String> topics = new ArrayList<>();
        topics.add(topicName);

        DeleteTopicsResult deleteTopicsResult = getAdminClient().deleteTopics(topics);

        for (Map.Entry<String, KafkaFuture<Void>> entry : deleteTopicsResult.values().entrySet()) {
            System.out.println("Topic("+entry.getKey()+") is deleted");
        }
    }

    public void listTopics() throws ExecutionException, InterruptedException {
        ListTopicsResult listTopicsResult =  getAdminClient().listTopics();
        KafkaFuture<Set<String>> names = listTopicsResult.names();
        for (String topic : names.get()) {
            System.out.println(topic);
        }
    }

    public void listDetailedInfoAboutTopics(Set<String> topics) throws ExecutionException, InterruptedException {
        DescribeTopicsResult describeTopicsResult = getAdminClient().describeTopics(topics);
        Map<String, TopicDescription> topicDescriptionMap = describeTopicsResult.all().get();
        for (Map.Entry<String, TopicDescription> entry : topicDescriptionMap.entrySet()) {
            TopicDescription topicDescription = entry.getValue();
            System.out.println("Topic: " + entry.getKey() + " and description: " + topicDescription.toString());
        }
    }
}
