package com.example.kafkastreams.topology;

import com.example.kafkastreams.domain.Greeting;
import com.example.kafkastreams.serdes.SerdesFactory;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Printed;
import org.apache.kafka.streams.kstream.Produced;

@Slf4j
public class GreetingTopology {

    public static String GREETINGS = "greetings";

    public static String GREETINGS_UPPERCASE = "greetings_uppercase";

    public static String GREETINGS_SPANISH = "greetings_spanish";

    public static Topology buildTopology(){

        // build pipeline of topology
        StreamsBuilder streamsBuilder = new StreamsBuilder();
        var mergedStream = getCustomGreetingKStream(streamsBuilder);

        mergedStream.print(Printed.<String,Greeting>toSysOut().withLabel("mergedStream"));

        // modifying stream
        var modifiedStream = mergedStream
                .filter((key,value) -> value.message().length() > 5)
                .peek((key, value) -> log.info("After filter key -> {} and value -> {}", key,value))
                .mapValues((readOnlyKey, value) -> new Greeting(value.message().toUpperCase(), value.timeStamp()));

        modifiedStream.print(Printed.<String,Greeting>toSysOut().withLabel("modifiedStream"));

        // writing to GREETINGS_UPPERCASE topic
        modifiedStream
                .to(GREETINGS_UPPERCASE, Produced.with(Serdes.String(), SerdesFactory.greetingSerdes()));


        return streamsBuilder.build();
    }



    private static KStream<String, Greeting> getCustomGreetingKStream(StreamsBuilder streamsBuilder){

        var greetingsStream = streamsBuilder
                .stream(GREETINGS, Consumed.with(Serdes.String(), SerdesFactory.greetingSerdes()));

        var greetingsSpanishStream = streamsBuilder
                .stream(GREETINGS_SPANISH,  Consumed.with(Serdes.String(), SerdesFactory.greetingSerdes()));
        return greetingsStream.merge(greetingsSpanishStream);
    }
}
