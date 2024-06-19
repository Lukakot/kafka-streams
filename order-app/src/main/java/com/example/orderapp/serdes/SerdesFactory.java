package com.example.orderapp.serdes;

import com.example.orderapp.domain.Order;
import com.example.orderapp.domain.Revenue;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;


public class SerdesFactory {

    public static Serde<Order> orderSerdes(){
        JsonSerializer<Order> jsonSerializer = new JsonSerializer<>();
        JsonDeserializer<Order> jsonDeserializer = new JsonDeserializer<>(Order.class);

        return Serdes.serdeFrom(jsonSerializer, jsonDeserializer);
    }

    public static Serde<Revenue> revenueSerdes(){
        JsonSerializer<Revenue> jsonSerializer = new JsonSerializer<>();
        JsonDeserializer<Revenue> jsonDeserializer = new JsonDeserializer<>(Revenue.class);

        return Serdes.serdeFrom(jsonSerializer, jsonDeserializer);
    }
}
