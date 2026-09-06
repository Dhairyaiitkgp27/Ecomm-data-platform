-- ---------------------------------------------------------------------------
-- SCD Type 2 demonstration helper.
--
-- Run order:
--   1) dbt snapshot                      -- captures v1 of every seller
--   2) psql ... -f sql/scd2_demo_update.sql   -- change one seller's attributes
--   3) dbt snapshot                      -- captures v2; closes v1
--   4) query dim_seller_history for that seller -> two versions, one is_current
-- ---------------------------------------------------------------------------
UPDATE staging.sellers
SET seller_rating = LEAST(5.0, seller_rating + 0.4),
    seller_city   = 'Pune',
    business_type = 'Brand Store'
WHERE seller_id = (SELECT seller_id FROM staging.sellers ORDER BY seller_id LIMIT 1);

-- Inspect the effect after re-running `dbt snapshot`:
--   SELECT seller_id, seller_rating, seller_city, business_type,
--          effective_start_date, effective_end_date, is_current
--   FROM   marts.dim_seller_history
--   WHERE  seller_id = (SELECT MIN(seller_id) FROM marts.dim_seller_history)
--   ORDER  BY effective_start_date;
