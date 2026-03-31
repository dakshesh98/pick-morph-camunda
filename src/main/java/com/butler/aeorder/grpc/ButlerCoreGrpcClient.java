package com.butler.aeorder.grpc;

import com.greyorange.butler.core.grpc.ButlerCoreServiceGrpc;
import com.greyorange.butler.core.grpc.GetPickInstructionStatusRequest;
import com.greyorange.butler.core.grpc.GetPickInstructionStatusResponse;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.net.InetAddress;
import java.util.concurrent.TimeUnit;

@Component
@Slf4j
public class ButlerCoreGrpcClient {

    @Value("${butler.core.grpc.address:gmc_butler_server:9090}")
    private String address;

    @Value("${butler.core.grpc.timeout-seconds:30}")
    private int timeoutSeconds;

    @Value("${butler.core.grpc.mock-enabled:true}")
    private boolean mockEnabled;

    private ManagedChannel channel;
    private ButlerCoreServiceGrpc.ButlerCoreServiceBlockingStub blockingStub;

    @PostConstruct
    public void init() {
        log.info("Initializing Butler Core gRPC client - address: {}, mockEnabled: {}", address, mockEnabled);

        if (!mockEnabled) {
            try {
                String[] parts = address.split(":");
                String host = parts[0];
                int port = parts.length > 1 ? Integer.parseInt(parts[1]) : 9090;

                // Resolve hostname to IP to bypass gRPC's strict DNS name validation.
                // Docker hostnames with underscores (e.g. gmc_butler_server) are invalid
                // per DNS RFC but Docker's internal DNS resolves them fine via Java's
                // InetAddress. We pass the resolved IP to gRPC instead.
                InetAddress resolved = InetAddress.getByName(host);
                String resolvedIp = resolved.getHostAddress();
                log.info("Resolved Butler Core host '{}' to IP '{}'", host, resolvedIp);

                channel = ManagedChannelBuilder.forAddress(resolvedIp, port)
                        .usePlaintext() // Use TLS in production
                        .keepAliveTime(30, TimeUnit.SECONDS)
                        .keepAliveTimeout(10, TimeUnit.SECONDS)
                        .build();
            } catch (Exception e) {
                throw new RuntimeException("Failed to initialize Butler Core gRPC channel for address: " + address, e);
            }

            blockingStub = ButlerCoreServiceGrpc.newBlockingStub(channel);
            log.info("Butler Core gRPC client initialized successfully");
        } else {
            log.warn("Butler Core gRPC client running in MOCK mode - will return simulated responses");
        }
    }

    @PreDestroy
    public void shutdown() {
        log.info("Shutting down Butler Core gRPC client");
        if (channel != null && !channel.isShutdown()) {
            try {
                channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                log.warn("Interrupted while shutting down gRPC channel", e);
                channel.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }
    }

    /**
     * Get pick instruction status from Butler Core.
     *
     * @param pickInstructionId the pick instruction ID
     * @return GetPickInstructionStatusResponse with current status
     */
    public GetPickInstructionStatusResponse getPickInstructionStatus(String pickInstructionId) {
        log.info("Getting pick instruction status from Butler Core - pickInstructionId: {}", pickInstructionId);

        // Return mock response if mock mode is enabled
        if (mockEnabled) {
            log.info("MOCK MODE: Returning simulated status for pick instruction");
            return GetPickInstructionStatusResponse.newBuilder()
                    .setPickId(pickInstructionId)
                    .setStatus("IN_PROGRESS")
                    .setIsComplete(false)
                    .setTotalQty(10)
                    .setProcessedQty(0)
                    .setRemainingQty(10)
                    .build();
        }

        GetPickInstructionStatusRequest request = GetPickInstructionStatusRequest.newBuilder()
                .setPickId(pickInstructionId)
                .build();

        try {
            GetPickInstructionStatusResponse response = blockingStub
                    .withDeadlineAfter(timeoutSeconds, TimeUnit.SECONDS)
                    .getPickInstructionStatus(request);

            log.info("Butler Core status response - pickInstructionId: {}, status: {}, isComplete: {}",
                    response.getPickId(), response.getStatus(), response.getIsComplete());

            return response;

        } catch (StatusRuntimeException e) {
            log.error("gRPC call to get pick instruction status failed - status: {}, description: {}",
                    e.getStatus().getCode(), e.getStatus().getDescription(), e);
            throw new RuntimeException("Failed to get pick instruction status from Butler Core: " + e.getMessage(), e);
        }
    }

}
