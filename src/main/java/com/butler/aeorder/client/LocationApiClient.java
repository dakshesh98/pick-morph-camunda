package com.butler.aeorder.client;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.butler.aeorder.dto.AePickListRequest;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

/**
 * HTTP client for the location-service.
 *
 * Fetches physical slot location data and destination group used to populate
 * AePickListRequest fields. Non-fatal: returns null on any error — the workflow
 * must continue without this data.
 */
@Component
@Slf4j
public class LocationApiClient {

    private final RestTemplate restTemplate;
    private final ObjectMapper objectMapper;
    private final String baseUrl;

    public LocationApiClient(RestTemplate restTemplate,
                             ObjectMapper objectMapper,
                             @Value("${location.service.host}") String host,
                             @Value("${location.service.port}") int port) {
        this.restTemplate = restTemplate;
        this.objectMapper = objectMapper;
        this.baseUrl = "http://" + host + ":" + port;
    }

    public AePickListRequest.AeLocation getLocation(String slotId) {
        String url = baseUrl + "/api/location/" + slotId;
        log.info("Fetching location data from location-service | url: {}", url);
        try {
            String raw = restTemplate.getForObject(url, String.class);
            if (raw == null || raw.isBlank()) {
                log.warn("Empty response from location-service for slotId: {}", slotId);
                return null;
            }
            JsonNode node = objectMapper.readTree(raw);
            JsonNode af = node.path("addressFields");
            AePickListRequest.AeLocation location = AePickListRequest.AeLocation.builder()
                    .displayName(node.path("displayName").asText(null))
                    .fullAddress(node.path("fullAddress").asText(null))
                    .addressFields(AePickListRequest.AeAddressFields.builder()
                            .side(af.path("side").asText(null))
                            .zone(af.path("zone").asText(null))
                            .level(af.path("level").asText(null))
                            .bay(af.path("bay").asText(null))
                            .aisle(af.path("aisle").asText(null))
                            .build())
                    .build();
            log.debug("Location fetched for slotId: {} | displayName: {}", slotId, location.getDisplayName());
            return location;
        } catch (Exception e) {
            log.warn("Failed to fetch location for slotId: {} — {}", slotId, e.getMessage());
            return null;
        }
    }

    public String getDestinationGroup(String binId) {
        String url = baseUrl + "/api/location/" + binId + "/destination-group";
        log.info("Fetching destination group from location-service | url: {}", url);
        try {
            String raw = restTemplate.getForObject(url, String.class);
            if (raw == null || raw.isBlank()) {
                log.warn("Empty response from location-service for destination_group | binId: {}", binId);
                return null;
            }
            JsonNode node = objectMapper.readTree(raw);
            String destinationGroup = node.path("destination_group").asText(null);
            log.debug("Destination group fetched for binId: {} | value: {}", binId, destinationGroup);
            return destinationGroup;
        } catch (Exception e) {
            log.warn("Failed to fetch destination_group for binId: {} — {}", binId, e.getMessage());
            return null;
        }
    }
}
