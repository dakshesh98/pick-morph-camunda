-- Drop old single-blob table (created by Hibernate auto-DDL in prior versions)
DROP TABLE IF EXISTS ae_order;

-- New structured table: same table stores both PICK (parent) and PICK_LINE (child) orders
CREATE TABLE ae_order (
    id                          BIGSERIAL PRIMARY KEY,
    external_service_request_id TEXT NOT NULL UNIQUE,
    type                        TEXT NOT NULL,
    fulfillment_area            JSONB,
    attributes                  JSONB,
    expectations                JSONB,
    state                       TEXT NOT NULL DEFAULT 'CREATED',
    created_at                  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at                  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_ae_order_state ON ae_order(state);
CREATE INDEX idx_ae_order_type  ON ae_order(type);

-- Maps each PICK_LINE child to its PICK parent
CREATE TABLE ae_orders_mapping (
    id                                     BIGSERIAL PRIMARY KEY,
    parent_external_service_request_id     TEXT NOT NULL REFERENCES ae_order(external_service_request_id),
    child_external_service_request_id      TEXT NOT NULL REFERENCES ae_order(external_service_request_id),
    UNIQUE (parent_external_service_request_id, child_external_service_request_id)
);

CREATE INDEX idx_ae_orders_mapping_parent ON ae_orders_mapping(parent_external_service_request_id);
CREATE INDEX idx_ae_orders_mapping_child  ON ae_orders_mapping(child_external_service_request_id);
