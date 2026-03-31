package com.butler.aeorder.controller;

import com.butler.aeorder.metrics.MetricsRegistry;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

@RestController
@RequestMapping("/ratelimit")
public class RateLimitMetricsController {

    private final MetricsRegistry registry;

    public RateLimitMetricsController(MetricsRegistry registry) {
        this.registry = registry;
    }

    @GetMapping("/metrics")
    public ResponseEntity<Map<String, Long>> metrics() {
        return ResponseEntity.ok(registry.snapshot());
    }
}
