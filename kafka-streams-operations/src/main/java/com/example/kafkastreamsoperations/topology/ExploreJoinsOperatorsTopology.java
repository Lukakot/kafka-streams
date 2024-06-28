package com.example.kafkastreamsoperations.topology;


import com.example.kafkastreamsoperations.domain.Alphabet;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;

import java.time.Duration;


@Slf4j
public class ExploreJoinsOperatorsTopology {

    //Topics
    public static String ALPHABETS = "alphabets"; // A => First letter in the english alphabet
    public static String ALPHABETS_ABBREVIATIONS = "alphabets_abbreviations"; // A=> Apple


    public static Topology build(){
        StreamsBuilder streamsBuilder = new StreamsBuilder();

        //joinKStreamWithKTable(streamsBuilder);
        //joinKStreamWithGlobalKTable(streamsBuilder);
        //joinKTableWithKTable(streamsBuilder);
        joinKStreamWithKStream(streamsBuilder);
        return streamsBuilder.build();
    }

    private static void joinKStreamWithKTable(StreamsBuilder streamsBuilder) {

        var alphabetAbbreviation = streamsBuilder
                .stream(ALPHABETS_ABBREVIATIONS,
                        Consumed.with(Serdes.String(),Serdes.String()));

        alphabetAbbreviation
                .print(Printed.<String,String>toSysOut().withLabel("alphabet_abbreviations"));


        var alphabetsTable = streamsBuilder
                .table(ALPHABETS,
                        Consumed.with(Serdes.String(),Serdes.String()),
                        Materialized.as("alphabet-store"));

        alphabetsTable
                .toStream()
                .print(Printed.<String,String>toSysOut().withLabel("alphabets"));

        ValueJoiner<String, String, Alphabet> valueJoiner = Alphabet::new;

        var joinedStream = alphabetAbbreviation
                .join(alphabetsTable, valueJoiner);

        joinedStream
                .print(Printed.<String,Alphabet>toSysOut().withLabel("alphabet-with-abbreviation"));
    }

    private static void joinKStreamWithKStream(StreamsBuilder streamsBuilder) {

        var alphabetAbbreviation = streamsBuilder
                .stream(ALPHABETS_ABBREVIATIONS,
                        Consumed.with(Serdes.String(),Serdes.String()));

        alphabetAbbreviation
                .print(Printed.<String,String>toSysOut().withLabel("alphabet_abbreviations"));


        var alphabetsStream = streamsBuilder
                .stream(ALPHABETS,
                        Consumed.with(Serdes.String(),Serdes.String()));

        alphabetsStream
                .print(Printed.<String,String>toSysOut().withLabel("alphabet"));

        ValueJoiner<String, String, Alphabet> valueJoiner = Alphabet::new;

        var fiveSecWindow = JoinWindows.ofTimeDifferenceWithNoGrace(Duration.ofSeconds(5));

        var joinedParams = StreamJoined.with(Serdes.String(), Serdes.String(), Serdes.String());

        var joinedStream = alphabetAbbreviation
                .join(alphabetsStream,
                        valueJoiner,
                        fiveSecWindow,
                        joinedParams);

        joinedStream
                .print(Printed.<String,Alphabet>toSysOut().withLabel("alphabet_joined_kstream_kstream"));
    }

    private static void joinKStreamWithGlobalKTable(StreamsBuilder streamsBuilder) {

        var alphabetAbbreviation = streamsBuilder
                .stream(ALPHABETS_ABBREVIATIONS,
                        Consumed.with(Serdes.String(),Serdes.String()));

        alphabetAbbreviation
                .print(Printed.<String,String>toSysOut().withLabel("alphabet_abbreviations"));


        var alphabetsTable = streamsBuilder
                .globalTable(ALPHABETS,
                        Consumed.with(Serdes.String(),Serdes.String()),
                        Materialized.as("alphabet-store"));

        ValueJoiner<String, String, Alphabet> valueJoiner = Alphabet::new;

        KeyValueMapper<String,String,String> keyValueMapper = (leftKey, rightKey) -> leftKey;

        var joinedStream = alphabetAbbreviation
                .join(alphabetsTable, keyValueMapper, valueJoiner);

        joinedStream
                .print(Printed.<String,Alphabet>toSysOut().withLabel("alphabet-with-abbreviation"));
    }

    private static void joinKTableWithKTable(StreamsBuilder streamsBuilder) {

        var alphabetAbbreviation = streamsBuilder
                .table(ALPHABETS_ABBREVIATIONS,
                        Consumed.with(Serdes.String(),Serdes.String()),
                        Materialized.as("alphabet_abbreviations-store"));

        alphabetAbbreviation
                .toStream()
                .print(Printed.<String,String>toSysOut().withLabel("alphabet_abbreviations"));


        var alphabetsTable = streamsBuilder
                .table(ALPHABETS,
                        Consumed.with(Serdes.String(),Serdes.String()),
                        Materialized.as("alphabet-store"));

        alphabetsTable
                .toStream()
                .print(Printed.<String,String>toSysOut().withLabel("alphabets"));

        ValueJoiner<String, String, Alphabet> valueJoiner = Alphabet::new;

        var joinedStream = alphabetAbbreviation
                .join(alphabetsTable, valueJoiner);

        joinedStream
                .toStream()
                .print(Printed.<String,Alphabet>toSysOut().withLabel("alphabet-with-abbreviation"));
    }
}
