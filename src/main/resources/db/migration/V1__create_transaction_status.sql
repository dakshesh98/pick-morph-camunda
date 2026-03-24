-- Flyway V1: create transaction_status table for durable transaction tracking
CREATE TABLE IF NOT EXISTS transaction_status (
    transaction_id VARCHAR(255) NOT NULL PRIMARY KEY,
    pick_instruction_id VARCHAR(255),
    status VARCHAR(50) NOT NULL,
    last_updated TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_transaction_status_pick_instruction_id ON transaction_status(pick_instruction_id);
