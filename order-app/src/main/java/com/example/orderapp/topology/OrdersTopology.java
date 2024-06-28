package com.example.orderapp.topology;

import com.example.orderapp.domain.Order;
import com.example.orderapp.domain.OrderType;
import com.example.orderapp.domain.Revenue;
import com.example.orderapp.domain.Store;
import com.example.orderapp.domain.TotalRevenue;
import com.example.orderapp.domain.TotalRevenueWithAddress;
import com.example.orderapp.serdes.SerdesFactory;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;

@Slf4j
public class OrdersTopology {
    public static final String ORDERS = "orders";
    public static final String STORES = "stores";

    public static final String GENERAL_ORDERS_COUNT = "general_orders_count";
    public static final String RESTAURANT_ORDERS_COUNT = "restaurant_orders_count";
    public static final String GENERAL_ORDERS_REVENUE = "general_orders_revenue";
    public static final String RESTAURANT_ORDERS_REVENUE = "restaurant_orders_revenue";
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

        var storesTable = streamsBuilder
                .table(STORES,
                        Consumed.with(Serdes.String(), SerdesFactory.storeSerdes()));


        ordersStream.split(Named.as("general-restaurant"))
                .branch(generalPredicate, Branched.withConsumer(generalOrderStream -> {
                            generalOrderStream.print(Printed.<String, Order>toSysOut().withLabel("generalStream"));
//                            generalOrderStream
//                                    .mapValues((key,value) -> revenueValueMapper.apply(value))
//                                    .to(GENERAL_ORDERS, Produced.with(Serdes.String(), SerdesFactory.revenueSerdes()));
                            aggregateOrdersByCount(generalOrderStream, GENERAL_ORDERS_COUNT);
                            aggregateOrdersByRevenue(generalOrderStream, GENERAL_ORDERS_REVENUE, storesTable);
                        })
                )
                .branch(restaurantPredicate, Branched.withConsumer(restaurantOrderStream -> {
                            restaurantOrderStream.print(Printed.<String, Order>toSysOut().withLabel("restaurantStream"));
//                            restaurantOrderStream
//                                    .mapValues((key,value) -> revenueValueMapper.apply(value))
//                                    .to(RESTAURANT_ORDERS, Produced.with(Serdes.String(), SerdesFactory.revenueSerdes()));
                            aggregateOrdersByCount(restaurantOrderStream, RESTAURANT_ORDERS_COUNT);
                            aggregateOrdersByRevenue(restaurantOrderStream, RESTAURANT_ORDERS_REVENUE, storesTable);
                        })
                );

        return streamsBuilder.build();
    }

    private static void aggregateOrdersByRevenue(KStream<String, Order> generalOrderStream, String storeName, KTable<String, Store> storesTable) {
            Initializer<TotalRevenue> totalRevenueInitializer =
                    TotalRevenue::new;

            Aggregator<String, Order, TotalRevenue> aggregator = (key, value, aggregate) -> aggregate.updateRunningRevenue(key, value);

            var revenueTable = generalOrderStream
                    .map((key, value) -> KeyValue.pair(value.locationId(), value))
                    .groupByKey(Grouped.with(Serdes.String(), SerdesFactory.orderSerdes()))
                    .aggregate(totalRevenueInitializer,
                            aggregator,
                            Materialized.<String,TotalRevenue, KeyValueStore<Bytes,byte[]>>as(storeName)
                                    .withKeySerde(Serdes.String())
                                    .withValueSerde(SerdesFactory.totalRevenueSerdes())
                    );

            //KTable-KTable
            ValueJoiner<TotalRevenue,Store,TotalRevenueWithAddress> valueJoiner = TotalRevenueWithAddress::new;

            var revenueAndStoreTable = revenueTable
                    .join(storesTable, valueJoiner);

            revenueAndStoreTable
                    .toStream()
                    .print(Printed.<String, TotalRevenueWithAddress>toSysOut().withLabel(storeName+"-bystore"));
    }

    private static void aggregateOrdersByCount(KStream<String, Order> generalOrderStream, String storeName) {
        var orderCountPerStore = generalOrderStream
                .map((key, value) -> KeyValue.pair(value.locationId(), value))
                .groupByKey(Grouped.with(Serdes.String(), SerdesFactory.orderSerdes()))
                .count(Named.as(storeName), Materialized.as(storeName));

        orderCountPerStore
                .toStream()
                .print(Printed.<String, Long>toSysOut().withLabel(storeName));
    }
}
