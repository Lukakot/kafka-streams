package com.example.orderapp.serdes;

import com.example.orderapp.domain.Order;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;


public class SerdesFactory {

    public static Serde<Order> orderSerdes(){
        JsonSerializer<Order> jsonSerializer = new JsonSerializer<>();
        JsonDeserializer<Order> jsonDeserializer = new JsonDeserializer<>(Order.class);

        return Serdes.serdeFrom(jsonSerializer, jsonDeserializer);
    }
}
