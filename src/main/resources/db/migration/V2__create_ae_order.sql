-- ae_order stores the PickListRequest payload sent to AE, enriched with state/actuals from events.
-- external_service_request_id = pickId (matches PickListRequest.externalServiceRequestId)
CREATE TABLE ae_order (
    external_service_request_id VARCHAR(255) PRIMARY KEY,
    payload                     JSONB        NOT NULL,
    status                      VARCHAR(50)  NOT NULL DEFAULT 'created',
    state                       VARCHAR(255),
    created_at                  TIMESTAMP WITH TIME ZONE NOT NULL,
    updated_at                  TIMESTAMP WITH TIME ZONE NOT NULL
);
