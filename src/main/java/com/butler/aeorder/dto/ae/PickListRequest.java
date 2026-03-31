package com.butler.aeorder.dto.ae;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

/**
 * AE contract DTO for <tenant>.pick-list.requests topic.
 * Published to AE to initiate a pick.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class PickListRequest {

    @JsonProperty("externalServiceRequestId")
    private String externalServiceRequestId;

    @JsonProperty("type")
    private String type;

    @JsonProperty("fulfillmentArea")
    private List<String> fulfillmentArea;

    @JsonProperty("attributes")
    private TopLevelAttributes attributes;

    @JsonProperty("serviceRequests")
    private List<ServiceRequest> serviceRequests;

    // ── Top-level attributes ────────────────────────────────────────────────

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class TopLevelAttributes {

        @JsonProperty("skipStandardPickProcess")
        private boolean skipStandardPickProcess;

        @JsonProperty("simple_priority")
        private String simplePriority;

        @JsonProperty("order_options")
        private OrderOptions orderOptions;
    }

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class OrderOptions {

        @JsonProperty("customer_order_info")
        private CustomerOrderInfo customerOrderInfo;

        @JsonProperty("palletization")
        private boolean palletization;

        @JsonProperty("order_clubbing")
        private boolean orderClubbing;

        @JsonProperty("order_splitting")
        private boolean orderSplitting;

        @JsonProperty("orderline_splitting")
        private boolean orderlineSplitting;

        @JsonProperty("grouping_tags")
        private GroupingTags groupingTags;

        @JsonProperty("bintags")
        private List<Object> bintags;

        @JsonProperty("container_type")
        private String containerType;

        @JsonProperty("destination_group")
        private String destinationGroup;

        @JsonProperty("simple_priority")
        private String simplePriority;

        @JsonProperty("behaviours")
        private List<String> behaviours;
    }

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class CustomerOrderInfo {

        @JsonProperty("order_id")
        private String orderId;

        @JsonProperty("order_line_id")
        private String orderLineId;

        @JsonProperty("master_order_id")
        private String masterOrderId;

        @JsonProperty("shipment_id")
        private String shipmentId;
    }

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class GroupingTags {

        @JsonProperty("mission_grouning_tag")
        private String missionGrouningTag;

        @JsonProperty("container_grouping_tag")
        private String containerGroupingTag;

        @JsonProperty("bin_grouping_tag")
        private String binGroupingTag;
    }

    // ── Service request (order line) ────────────────────────────────────────

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class ServiceRequest {

        @JsonProperty("externalServiceRequestId")
        private String externalServiceRequestId;

        @JsonProperty("type")
        private String type;

        @JsonProperty("attributes")
        private ServiceRequestAttributes attributes;

        @JsonProperty("expectations")
        private Expectations expectations;
    }

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class ServiceRequestAttributes {

        @JsonProperty("extra_info")
        private ExtraInfo extraInfo;

        @JsonProperty("shelfLifeHours")
        private int shelfLifeHours;

        @JsonProperty("location")
        private Location location;
    }

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class ExtraInfo {

        @JsonProperty("client_task_id")
        private String clientTaskId;
    }

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class Location {

        @JsonProperty("displayName")
        private String displayName;

        @JsonProperty("fullAddress")
        private String fullAddress;

        @JsonProperty("addressFields")
        private AddressFields addressFields;
    }

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class AddressFields {

        @JsonProperty("side")
        private String side;

        @JsonProperty("zone")
        private String zone;

        @JsonProperty("level")
        private String level;

        @JsonProperty("bay")
        private String bay;

        @JsonProperty("aisle")
        private String aisle;
    }

    // ── Expectations ────────────────────────────────────────────────────────

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class Expectations {

        @JsonProperty("containers")
        private List<Container> containers;
    }

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class Container {

        @JsonProperty("products")
        private List<Product> products;
    }

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class Product {

        @JsonProperty("productQuantity")
        private int productQuantity;

        @JsonProperty("productAttributes")
        private ProductAttributes productAttributes;
    }

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class ProductAttributes {

        @JsonProperty("lot_id")
        private String lotId;

        @JsonProperty("filter_parameters")
        private List<String> filterParameters;

        @JsonProperty("barcodes")
        private List<String> barcodes;

        @JsonProperty("product_sku")
        private String productSku;

        @JsonProperty("package_parameters")
        private List<String> packageParameters;
    }
}
