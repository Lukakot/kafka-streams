package com.example.orderapp.util;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.processor.TimestampExtractor;
import com.example.orderapp.domain.Order;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;

@Slf4j
public class OrderTimeStampExtractor implements TimestampExtractor {

    @Override
    public long extract(ConsumerRecord<Object, Object> consumerRecord, long partitionTime) {
        var order = (Order) consumerRecord.value();
        if(order!= null && order.orderedDateTime()!=null){
            var timeStamp = order.orderedDateTime();
            log.info("timestamp in extractor: {}", timeStamp);
            return convertToInstantFromUTC(timeStamp);
        }
        return partitionTime;
    }

    private long convertToInstantFromUTC(LocalDateTime timeStamp) {
        ZoneId utcPlus2 = ZoneId.of("UTC+02:00");
        ZonedDateTime zonedDateTime = ZonedDateTime.of(timeStamp, utcPlus2);
        return zonedDateTime.toInstant().toEpochMilli();
    }
}
