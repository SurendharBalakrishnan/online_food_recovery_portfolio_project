# QuickBite Express - Data Cleaning Log

**Date:** 2026-02-11 13:33:18

## Actions Taken

1. **fact_ratings**: Dropped null rows and 16 duplicate order_ids (kept first)
2. **fact_orders.delivery_partner_id**: Filled 5,635 nulls with 'UNKNOWN'
3. **dim_customer**: Added 4,930 placeholder records for orphan customer_ids
4. **fact_order_items**: Removed ~16,425 rows with orphan order_ids
5. **fact_orders -> fact_ratings**: No action (expected: not all orders rated)
6. **Data types**: Converted timestamps, added phase/month helper columns

## Final Quality Score: 99.94%
