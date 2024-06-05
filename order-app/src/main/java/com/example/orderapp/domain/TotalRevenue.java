package com.example.orderapp.domain;

import java.math.BigDecimal;

public record TotalRevenue(String locationId,
                           Integer runnuingOrderCount,
                           BigDecimal runningRevenue) {
}
