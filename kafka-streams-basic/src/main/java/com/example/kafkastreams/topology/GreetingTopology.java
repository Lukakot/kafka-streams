package com.example.kafkastreams.topology;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Printed;
import org.apache.kafka.streams.kstream.Produced;

public class GreetingTopology {

    public static String GREETINGS = "greetings";

    public static String GREETINGS_UPPERCASE = "greetings_uppercase";

    public static Topology buildTopology(){

        // build pipeline of topology (
        StreamsBuilder streamsBuilder = new StreamsBuilder();

        // read from GREETINGS topic
        var greetingsStream = streamsBuilder
                .stream(GREETINGS, Consumed.with(Serdes.String(), Serdes.String()));

        greetingsStream.print(Printed.<String,String>toSysOut().withLabel("greetingsStream"));
        // modifying stream
        var modifiedStream = greetingsStream
                .mapValues((readOnlyKey, value) -> value.toUpperCase());

        modifiedStream.print(Printed.<String,String>toSysOut().withLabel("modifiedStream"));
        // writing to GREETINGS_UPPERCASE topic
        modifiedStream
                .to(GREETINGS_UPPERCASE, Produced.with(Serdes.String(), Serdes.String()));


        return streamsBuilder.build();
    }
}
