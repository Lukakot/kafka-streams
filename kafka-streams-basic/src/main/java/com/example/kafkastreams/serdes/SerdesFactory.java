package com.example.kafkastreams.serdes;

import com.example.kafkastreams.domain.Greeting;
import org.apache.kafka.common.serialization.Serde;

public class SerdesFactory {

    public static Serde<Greeting> greetingSerdes(){
        return new GreetingSerdes();
    }
}
