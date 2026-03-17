-- Read-optimization indexes for staging tables
-- Apply to both production and sandbox copies.

-- Date range filter acceleration
CREATE INDEX IF NOT EXISTS staging_cust360_trans_posted_dt_idx
ON moneymovement_source.staging_cust360_corp_credit_card_trans (transaction_posted_dt);

CREATE INDEX IF NOT EXISTS staging_cust360_trans_posted_dt_sandbox_idx
ON moneymovement_source.staging_cust360_corp_credit_card_trans_sandbox (transaction_posted_dt);

-- Optional: precomputed party_id lookup
CREATE INDEX IF NOT EXISTS staging_cust360_party_id_idx
ON moneymovement_source.staging_cust360_corp_credit_card_trans (party_id);

CREATE INDEX IF NOT EXISTS staging_cust360_party_id_sandbox_idx
ON moneymovement_source.staging_cust360_corp_credit_card_trans_sandbox (party_id);
