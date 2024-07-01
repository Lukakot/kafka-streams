package com.example.orderapp.topology;

import com.example.orderapp.domain.Order;
import com.example.orderapp.domain.OrderType;
import com.example.orderapp.domain.Revenue;
import com.example.orderapp.domain.Store;
import com.example.orderapp.domain.TotalRevenue;
import com.example.orderapp.domain.TotalRevenueWithAddress;
import com.example.orderapp.serdes.SerdesFactory;
import com.example.orderapp.util.OrderTimeStampExtractor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.WindowStore;

import java.time.Duration;

@Slf4j
public class OrdersTopology {
    public static final String ORDERS = "orders";
    public static final String STORES = "stores";

    public static final String GENERAL_ORDERS_COUNT = "general_orders_count";
    public static final String GENERAL_ORDERS_COUNT_WINDOW = "general_orders_count_window";
    public static final String RESTAURANT_ORDERS_COUNT = "restaurant_orders_count";
    public static final String RESTAURANT_ORDERS_COUNT_WINDOW = "restaurant_orders_count_window";
    public static final String GENERAL_ORDERS_REVENUE = "general_orders_revenue";
    public static final String GENERAL_ORDERS_REVENUE_WINDOW = "general_orders_revenue_window";
    public static final String RESTAURANT_ORDERS_REVENUE = "restaurant_orders_revenue";
    public static final String RESTAURANT_ORDERS_REVENUE_WINDOW = "restaurant_orders_revenue_window";
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
                        Consumed.with(Serdes.String(), SerdesFactory.storeSerdes())
                                .withTimestampExtractor(new OrderTimeStampExtractor()));


        ordersStream.split(Named.as("general-restaurant"))
                .branch(generalPredicate, Branched.withConsumer(generalOrderStream -> {
                            generalOrderStream.print(Printed.<String, Order>toSysOut().withLabel("generalStream"));
                            generalOrderStream
                                    .mapValues((key,value) -> revenueValueMapper.apply(value))
                                    .to(GENERAL_ORDERS, Produced.with(Serdes.String(), SerdesFactory.revenueSerdes()));
                           // aggregateOrdersByCount(generalOrderStream, GENERAL_ORDERS_COUNT);
                           // aggregateOrdersCountByTimeWindow(generalOrderStream, GENERAL_ORDERS_COUNT_WINDOW, storesTable);
                           // aggregateOrdersByRevenue(generalOrderStream, GENERAL_ORDERS_REVENUE, storesTable);
                            aggregateOrdersByRevenueByTimeWindow(generalOrderStream, GENERAL_ORDERS_REVENUE_WINDOW, storesTable);
                        })
                )
                .branch(restaurantPredicate, Branched.withConsumer(restaurantOrderStream -> {
                            restaurantOrderStream.print(Printed.<String, Order>toSysOut().withLabel("restaurantStream"));
                            restaurantOrderStream
                                    .mapValues((key,value) -> revenueValueMapper.apply(value))
                                    .to(RESTAURANT_ORDERS, Produced.with(Serdes.String(), SerdesFactory.revenueSerdes()));
                          //  aggregateOrdersByCount(restaurantOrderStream, RESTAURANT_ORDERS_COUNT);
                          //  aggregateOrdersCountByTimeWindow(restaurantOrderStream, RESTAURANT_ORDERS_COUNT_WINDOW, storesTable);
                          //  aggregateOrdersByRevenue(restaurantOrderStream, RESTAURANT_ORDERS_REVENUE, storesTable);
                            aggregateOrdersByRevenueByTimeWindow(restaurantOrderStream, RESTAURANT_ORDERS_REVENUE_WINDOW, storesTable);
                        })
                );

        return streamsBuilder.build();
    }

    private static void aggregateOrdersByRevenueByTimeWindow(KStream<String, Order> generalOrderStream, String storeName, KTable<String, Store> storesTable) {

        var windowSize = Duration.ofSeconds(5);
        var timeWindows = TimeWindows.ofSizeWithNoGrace(windowSize);
        Initializer<TotalRevenue> totalRevenueInitializer = TotalRevenue::new;

        Aggregator<String, Order, TotalRevenue> aggregator = (key, value, aggregate) -> aggregate.updateRunningRevenue(key, value);

        var revenueTable = generalOrderStream
                .map((key, value) -> KeyValue.pair(value.locationId(), value))
                .groupByKey(Grouped.with(Serdes.String(), SerdesFactory.orderSerdes()))
                .windowedBy(timeWindows)
                .aggregate(totalRevenueInitializer,
                        aggregator,
                        Materialized.<String,TotalRevenue, WindowStore<Bytes,byte[]>>as(storeName)
                                .withKeySerde(Serdes.String())
                                .withValueSerde(SerdesFactory.totalRevenueSerdes())
                );

        revenueTable
                .toStream()
                .peek((key, value) -> {
                    log.info("Store Name: {}, key: {}, value: {}", storeName, key, value);
                })
                .print(Printed.<Windowed<String>, TotalRevenue>toSysOut().withLabel(storeName));
    }


    private static void aggregateOrdersCountByTimeWindow(KStream<String, Order> generalOrderStream, String storeName, KTable<String, Store> storesTable) {

        var windowSize = Duration.ofSeconds(15);
        var timeWindows = TimeWindows.ofSizeWithNoGrace(windowSize);
        var orderCountPerStore = generalOrderStream
                .map((key, value) -> KeyValue.pair(value.locationId(), value))
                .groupByKey(Grouped.with(Serdes.String(), SerdesFactory.orderSerdes()))
                .windowedBy(timeWindows)
                .count(Named.as(storeName), Materialized.as(storeName))
                .suppress(Suppressed.untilWindowCloses(Suppressed.BufferConfig.unbounded().shutDownWhenFull()));

        orderCountPerStore
                .toStream()
                .peek((key, value) -> {
                    log.info("Store Name: {}, key: {}, value: {}", storeName, key, value);
                })
                .print(Printed.<Windowed<String>, Long>toSysOut().withLabel(storeName));

//        ValueJoiner<TotalRevenue,Store,TotalRevenueWithAddress> valueJoiner = TotalRevenueWithAddress::new;
//        var revenueAndStoreTable = orderCountPerStore
//                .join(storesTable, valueJoiner);
//
//        revenueAndStoreTable
//                .toStream()
//                .print(Printed.<String, TotalRevenueWithAddress>toSysOut().withLabel(storeName+"-bystore"));
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
