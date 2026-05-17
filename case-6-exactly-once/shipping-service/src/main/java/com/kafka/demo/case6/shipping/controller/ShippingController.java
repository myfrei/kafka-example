package com.kafka.demo.case6.shipping.controller;

import com.kafka.demo.case6.shipping.service.ShippingService;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

/** GET /api/shipments — отгруженные заказы. */
@RestController
@RequestMapping("/api/shipments")
@RequiredArgsConstructor
public class ShippingController {

    private final ShippingService shippingService;

    @GetMapping
    public ResponseEntity<Map<String, Object>> shipments() {
        return ResponseEntity.ok(Map.of(
            "shippedCount", shippingService.shippedCount(),
            "shippedOrders", shippingService.shippedOrders()
        ));
    }
}
