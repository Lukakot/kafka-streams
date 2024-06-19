package com.example.orderapp.topology;

import com.example.orderapp.domain.Order;
import com.example.orderapp.domain.OrderType;
import com.example.orderapp.domain.Revenue;
import com.example.orderapp.serdes.SerdesFactory;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;

@Slf4j
public class OrdersTopology {
    public static final String ORDERS = "orders";
    public static final String STORES = "stores";

    public static final String GENERAL_ORDERS = "general_orders";
    public static final String RESTAURANT_ORDERS = "restaurant_orders";

    public static Topology buildTopology(){

        Predicate<String, Order> generalPredicate = (string, order) -> order.orderType().equals(OrderType.GENERAL);
        Predicate<String, Order> restaurantPredicate = (string, order) -> order.orderType().equals(OrderType.RESTAURANT);

        ValueMapper<Order, Revenue> revenueValueMapper = order -> new Revenue(order.locationId(), order.finalAmount());
        StreamsBuilder streamsBuilder = new StreamsBuilder();

        var ordersStream = streamsBuilder.stream(ORDERS, Consumed.with(Serdes.String(), SerdesFactory.orderSerdes()));

        ordersStream.print(Printed.<String, Order>toSysOut()
                .withLabel("orders"));

        ordersStream.split(Named.as("general-restaurant"))
                .branch(generalPredicate, Branched.withConsumer(generalOrderStream -> {
                            generalOrderStream.print(Printed.<String, Order>toSysOut().withLabel("generalStream"));
                            generalOrderStream
                                    .mapValues((key,value) -> revenueValueMapper.apply(value))
                                    .to(GENERAL_ORDERS, Produced.with(Serdes.String(), SerdesFactory.revenueSerdes()));
                        })
                )
                .branch(restaurantPredicate, Branched.withConsumer(restaurantOrderStream -> {
                            restaurantOrderStream.print(Printed.<String, Order>toSysOut().withLabel("restaurantStream"));
                            restaurantOrderStream
                                    .mapValues((key,value) -> revenueValueMapper.apply(value))
                                    .to(RESTAURANT_ORDERS, Produced.with(Serdes.String(), SerdesFactory.revenueSerdes()));
                        })
                );

        return streamsBuilder.build();
    }
}
