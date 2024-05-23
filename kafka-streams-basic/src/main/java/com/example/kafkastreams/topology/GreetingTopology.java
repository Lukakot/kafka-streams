package com.example.kafkastreams.topology;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Printed;

@Slf4j
public class GreetingTopology {

    public static String GREETINGS = "greetings";

    public static String GREETINGS_UPPERCASE = "greetings_uppercase";

    public static String GREETINGS_SPANISH = "greetings_spanish";

    public static Topology buildTopology(){

        // build pipeline of topology (
        StreamsBuilder streamsBuilder = new StreamsBuilder();

        // read from GREETINGS topic
        KStream<String,String> greetingsStream = streamsBuilder
                .stream(GREETINGS);

        KStream<String,String> greetingsSpanishStream = streamsBuilder
                .stream(GREETINGS_SPANISH);

        var mergedStream = greetingsStream.merge(greetingsSpanishStream);

        mergedStream.print(Printed.<String,String>toSysOut().withLabel("mergedStream"));

        greetingsStream.print(Printed.<String,String>toSysOut().withLabel("greetingsStream"));
        // modifying stream
        KStream<String,String> modifiedStream = mergedStream
                .filter((key,value) -> value.length() > 5)
                .peek((key, value) -> log.info("After filter key -> {} and value -> {}", key,value))
                .mapValues((readOnlyKey, value) -> value.toUpperCase());

        modifiedStream.print(Printed.<String,String>toSysOut().withLabel("modifiedStream"));

        // writing to GREETINGS_UPPERCASE topic
        modifiedStream
                .to(GREETINGS_UPPERCASE);


        return streamsBuilder.build();
    }
}
