package com.example.kafkastreams.domain;

import java.time.LocalDateTime;

public record Greeting(String message, LocalDateTime timeStamp) {
}
