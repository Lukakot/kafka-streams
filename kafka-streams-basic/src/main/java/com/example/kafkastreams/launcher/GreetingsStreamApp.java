package com.example.kafkastreams.launcher;

import com.example.kafkastreams.topology.GreetingTopology;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;


import java.util.List;
import java.util.Properties;

@Slf4j
public class GreetingsStreamApp {

    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "greetings-app");
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");


        createTopics(properties, List.of(GreetingTopology.GREETINGS, GreetingTopology.GREETINGS_SPANISH, GreetingTopology.GREETINGS_UPPERCASE));
        var greetingsTopology = GreetingTopology.buildTopology();
        var kafkaStreams = new KafkaStreams(greetingsTopology, properties);

        //gracefully shutdown
        Runtime.getRuntime().addShutdownHook(new Thread(kafkaStreams::close));
        try {
            kafkaStreams.start();
        }catch (Exception e){
            log.error("Exception in starting stream -> {}", e.getMessage());
        }

    }

    private static void createTopics(Properties propertiesConfig, List<String> greetings){

        AdminClient client = AdminClient.create(propertiesConfig);
        int partitions = 2;
        short replication = 1;

        var newTopics = greetings
                .stream()
                .map(topic -> new NewTopic(topic, partitions, replication))
                .toList();

        var createTopicResult = client.createTopics(newTopics);
        try{
            createTopicResult
                    .all().get();
            log.info("topics created successfully");
        }catch (Exception e){
            log.error("Exception creating topics -> {}", e.getMessage(), e);
        }
    }
}
