-- ae_order: soft-delete, lifecycle stages, hold flag
ALTER TABLE ae_order
    ADD COLUMN is_deleted   BOOLEAN      NOT NULL DEFAULT FALSE,
    ADD COLUMN stages       JSONB        NOT NULL DEFAULT '[]',
    ADD COLUMN on_hold      BOOLEAN      NOT NULL DEFAULT FALSE;

-- ae_orders_mapping: audit timestamps
ALTER TABLE ae_orders_mapping
    ADD COLUMN created_at   TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    ADD COLUMN updated_at   TIMESTAMPTZ  NOT NULL DEFAULT NOW();
