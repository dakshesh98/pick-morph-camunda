package com.butler.aeorder.config.metrics;

import com.butler.aeorder.metrics.MetricsRegistry;
import jakarta.servlet.Filter;
import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.ServletRequest;
import jakarta.servlet.ServletResponse;
import jakarta.servlet.http.HttpServletRequest;
import org.springframework.stereotype.Component;

import java.io.IOException;

@Component
public class RequestMetricsFilter implements Filter {

    private final MetricsRegistry registry;

    public RequestMetricsFilter(MetricsRegistry registry) {
        this.registry = registry;
    }

    @Override
    public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain) throws IOException, ServletException {
        if (request instanceof HttpServletRequest) {
            HttpServletRequest req = (HttpServletRequest) request;
            String key = req.getMethod() + " " + req.getRequestURI();
            registry.increment(key);
        }
        chain.doFilter(request, response);
    }
}
