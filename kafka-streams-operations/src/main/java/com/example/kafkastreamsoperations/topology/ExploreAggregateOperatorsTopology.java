package com.example.kafkastreamsoperations.topology;

import com.example.kafkastreamsoperations.domain.AlphabetWordAggregate;
import com.example.kafkastreamsoperations.serdes.SerdesFactory;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;

@Slf4j
public class ExploreAggregateOperatorsTopology {


    public static String AGGREGATE = "aggregate";

    public static Topology build(){
        StreamsBuilder streamsBuilder = new StreamsBuilder();

        var inputStream = streamsBuilder
                .stream(AGGREGATE,
                        Consumed.with(Serdes.String(),Serdes.String()));

        inputStream
                .print(Printed.<String,String>toSysOut().withLabel(AGGREGATE));

        var groupedString = inputStream
                .groupByKey(Grouped.with(Serdes.String(), Serdes.String()));

        groupedStreamCount(groupedString);
        groupedStreamAggregate(groupedString);

        return streamsBuilder.build();
    }

    private static void groupedStreamAggregate(KGroupedStream<String, String> groupedString) {

        Initializer<AlphabetWordAggregate> alphabetWordAggregateInitializer = AlphabetWordAggregate::new;

        Aggregator<String, String, AlphabetWordAggregate> aggregator = (key, value, aggregate) -> aggregate.updateNewEvents(key,value);

        var aggregatedStream = groupedString
                .aggregate(alphabetWordAggregateInitializer, aggregator,
                        Materialized.<String, AlphabetWordAggregate, KeyValueStore<Bytes, byte[]>>as("aggregated-store")
                        .withKeySerde(Serdes.String())
                        .withValueSerde(SerdesFactory.alphabetWordAggregate()));

        aggregatedStream
                .toStream()
                .print(Printed.<String,AlphabetWordAggregate>toSysOut().withLabel("aggregated-words"));
    }

    private static void groupedStreamCount(KGroupedStream<String, String> groupedStream) {

        var countByAlphabet = groupedStream
                .count(Named.as("count-per-alphabet"));

        countByAlphabet
                .toStream()
                .print(Printed.<String,Long>toSysOut().withLabel("words-count-per-alphabet"));

    }

}
