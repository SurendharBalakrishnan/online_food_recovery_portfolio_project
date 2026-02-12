# QuickBite Express - Data Quality Report

**Generated:** 2026-02-11 12:55:04

**Data Quality Score:** 93.3%

---

## Dataset Overview

Table,Rows,Columns,Memory (MB)
fact_orders,"149,166",11,69.96
fact_order_items,"342,994",8,99.77
fact_ratings,"68,842",7,24.13
fact_delivery_performance,"149,166",4,13.66
dim_customer,"107,776",4,26.88
dim_restaurant,"19,995",7,8.78
dim_delivery_partner,"15,000",7,5.53
dim_menu_item,"342,671",6,110.07

## Null Analysis

Table,Column,Null Count,Null %
fact_orders,delivery_partner_id,5635,3.78
fact_order_items,None,0,0.0
fact_ratings,order_id,17,0.02
fact_ratings,customer_id,17,0.02
fact_ratings,restaurant_id,17,0.02
fact_ratings,rating,17,0.02
fact_ratings,review_text,17,0.02
fact_ratings,review_timestamp,17,0.02
fact_ratings,sentiment_score,17,0.02
fact_delivery_performance,None,0,0.0
dim_customer,None,0,0.0
dim_restaurant,None,0,0.0
dim_delivery_partner,None,0,0.0
dim_menu_item,None,0,0.0

## Duplicate Check

Table,Primary Key,Full Row Duplicates,PK Duplicates,Status
fact_orders,order_id,0,0,✅ Clean
fact_order_items,order_id + item_id,0,0,✅ Clean
fact_ratings,order_id,16,16,⚠️ Has Duplicates
fact_delivery_performance,order_id,0,0,✅ Clean
dim_customer,customer_id,0,0,✅ Clean
dim_restaurant,restaurant_id,0,0,✅ Clean
dim_delivery_partner,delivery_partner_id,0,0,✅ Clean
dim_menu_item,menu_item_id,0,0,✅ Clean

## Relationship Validation

Fact Table,FK Column,Dim Table,PK Column,Fact Unique Keys,Dim Unique Keys,Orphan Keys,Match %,Status
fact_orders,customer_id,dim_customer,customer_id,105180,107776,4930,95.31,⚠️ 4930 orphans
fact_orders,restaurant_id,dim_restaurant,restaurant_id,19983,19995,0,100.0,✅ Valid
fact_orders,delivery_partner_id,dim_delivery_partner,delivery_partner_id,15000,15000,0,100.0,✅ Valid
fact_orders,order_id,fact_delivery_performance,order_id,149166,149166,0,100.0,✅ Valid
fact_orders,order_id,fact_ratings,order_id,149166,68825,80341,46.14,⚠️ 80341 orphans
fact_order_items,order_id,fact_orders,order_id,154479,149166,16425,89.37,⚠️ 16425 orphans
fact_order_items,menu_item_id,dim_menu_item,menu_item_id,342671,342671,0,100.0,✅ Valid

---

**Completeness:** 99.93%
**PK Duplicates:** 16
**Unexpected Orphans:** 21355

**Status:** PROCEED TO ANALYSIS
