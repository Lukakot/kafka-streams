package com.example.kafkastreamsoperations.producer;

import lombok.extern.slf4j.Slf4j;

import java.util.Map;

import static com.example.kafkastreamsoperations.producer.ProducerUtil.publishMessageSync;
import static com.example.kafkastreamsoperations.topology.ExploreJoinsOperatorsTopology.ALPHABETS;
import static com.example.kafkastreamsoperations.topology.ExploreJoinsOperatorsTopology.ALPHABETS_ABBREVIATIONS;

@Slf4j
public class JoinsMockDataProducer {


    public static void main(String[] args){


        var alphabetMap = Map.of(
                "A", "A is the first letter in English Alphabets.",
                "B", "B is the second letter in English Alphabets."

        );
       publishMessages(alphabetMap, ALPHABETS);

       // sleep(6000);

        var alphabetAbbrevationMap = Map.of(
                "A", "Apple",
                "B", "Bus."
                ,"C", "Cat."

        );

        alphabetAbbrevationMap = Map.of(
                "A", "Airplane",
                "B", "Baby."
        );
        publishMessages(alphabetAbbrevationMap, ALPHABETS_ABBREVIATIONS);

    }

    private static void publishMessages(Map<String, String> alphabetMap, String topic) {

        alphabetMap
                .forEach((key, value) -> {
                    var recordMetaData = publishMessageSync(topic, key,value);
                    log.info("Published the alphabet message : {} ", recordMetaData);
                });
    }



}
