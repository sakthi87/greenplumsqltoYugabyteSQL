BEGIN;

-- Duplicate source table 1: staging transactions
DROP TABLE IF EXISTS moneymovement_source.staging_cust360_corp_credit_card_trans_sandbox;
CREATE TABLE moneymovement_source.staging_cust360_corp_credit_card_trans_sandbox
  (LIKE moneymovement_source.staging_cust360_corp_credit_card_trans INCLUDING ALL);
INSERT INTO moneymovement_source.staging_cust360_corp_credit_card_trans_sandbox
SELECT * FROM moneymovement_source.staging_cust360_corp_credit_card_trans;

-- Duplicate source table 2: current target snapshot (reference copy)
DROP TABLE IF EXISTS moneymovement_source.tbl_entityres_card_merchants_src_sandbox;
CREATE TABLE moneymovement_source.tbl_entityres_card_merchants_src_sandbox
  (LIKE moneymovement_source.tbl_entityres_card_merchants INCLUDING ALL);
INSERT INTO moneymovement_source.tbl_entityres_card_merchants_src_sandbox
SELECT * FROM moneymovement_source.tbl_entityres_card_merchants;

-- Duplicate target table: working copy for Spark job output
DROP TABLE IF EXISTS moneymovement_source.tbl_entityres_card_merchants_sandbox;
CREATE TABLE moneymovement_source.tbl_entityres_card_merchants_sandbox
  (LIKE moneymovement_source.tbl_entityres_card_merchants INCLUDING ALL);
INSERT INTO moneymovement_source.tbl_entityres_card_merchants_sandbox
SELECT * FROM moneymovement_source.tbl_entityres_card_merchants;

COMMIT;
