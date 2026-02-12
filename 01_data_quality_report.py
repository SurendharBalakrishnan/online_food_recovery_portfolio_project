"""
QuickBite Express - Sprint 1, Task 1: Data Quality Report
============================================================
Loads all 8 datasets, validates schema, checks nulls/duplicates/dtypes,
validates star schema relationships, and exports a summary report.

Output: /output/01_data_quality_report/
"""

import pandas as pd
import numpy as np
import os
from datetime import datetime

# ============================================================
# CONFIG - Change these paths to reuse for new clients
# ============================================================
DATA_DIR = "datasets/"
OUTPUT_DIR = "output/01_data_quality_report/"
os.makedirs(OUTPUT_DIR, exist_ok=True)

CRISIS_START = "2025-06-01"
CRISIS_END = "2025-09-30"
PRE_CRISIS_START = "2025-01-01"
PRE_CRISIS_END = "2025-05-31"

# ============================================================
# 1. LOAD ALL DATASETS
# ============================================================
print("=" * 70)
print("QUICKBITE EXPRESS - DATA QUALITY REPORT")
print(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print("=" * 70)

print("\n📂 Loading datasets...")

fact_orders = pd.read_csv(f"{DATA_DIR}fact_orders.csv")
fact_order_items = pd.read_csv(f"{DATA_DIR}fact_order_items.csv")
fact_ratings = pd.read_csv(f"{DATA_DIR}fact_ratings.csv")
fact_delivery = pd.read_csv(f"{DATA_DIR}fact_delivery_performance.csv")
dim_customer = pd.read_csv(f"{DATA_DIR}dim_customer.csv")
dim_restaurant = pd.read_csv(f"{DATA_DIR}dim_restaurant.csv")
dim_delivery_partner = pd.read_csv(f"{DATA_DIR}dim_delivery_partner_.csv")
dim_menu_item = pd.read_csv(f"{DATA_DIR}dim_menu_item.csv")

datasets = {
    "fact_orders": fact_orders,
    "fact_order_items": fact_order_items,
    "fact_ratings": fact_ratings,
    "fact_delivery_performance": fact_delivery,
    "dim_customer": dim_customer,
    "dim_restaurant": dim_restaurant,
    "dim_delivery_partner": dim_delivery_partner,
    "dim_menu_item": dim_menu_item,
}

print("✅ All 8 datasets loaded successfully!\n")

# ============================================================
# 2. BASIC OVERVIEW
# ============================================================
print("=" * 70)
print("TABLE 1: DATASET OVERVIEW")
print("=" * 70)

overview_data = []
for name, df in datasets.items():
    overview_data.append({
        "Table": name,
        "Rows": f"{len(df):,}",
        "Columns": df.shape[1],
        "Memory (MB)": round(df.memory_usage(deep=True).sum() / 1024**2, 2),
    })

overview_df = pd.DataFrame(overview_data)
print(overview_df.to_string(index=False))

# ============================================================
# 3. NULL VALUE CHECK
# ============================================================
print("\n" + "=" * 70)
print("TABLE 2: NULL VALUE ANALYSIS")
print("=" * 70)

null_data = []
for name, df in datasets.items():
    null_counts = df.isnull().sum()
    null_cols = null_counts[null_counts > 0]
    if len(null_cols) > 0:
        for col, count in null_cols.items():
            null_data.append({
                "Table": name,
                "Column": col,
                "Null Count": count,
                "Null %": round(count / len(df) * 100, 2),
            })
    else:
        null_data.append({
            "Table": name,
            "Column": "None",
            "Null Count": 0,
            "Null %": 0.0,
        })

null_df = pd.DataFrame(null_data)
print(null_df.to_string(index=False))

# ============================================================
# 4. DUPLICATE CHECK
# ============================================================
print("\n" + "=" * 70)
print("TABLE 3: DUPLICATE CHECK")
print("=" * 70)

pk_map = {
    "fact_orders": "order_id",
    "fact_order_items": None,  # composite key (order_id + item_id)
    "fact_ratings": "order_id",
    "fact_delivery_performance": "order_id",
    "dim_customer": "customer_id",
    "dim_restaurant": "restaurant_id",
    "dim_delivery_partner": "delivery_partner_id",
    "dim_menu_item": "menu_item_id",
}

dup_data = []
for name, df in datasets.items():
    pk = pk_map[name]
    full_dups = df.duplicated().sum()
    if pk:
        pk_dups = df[pk].duplicated().sum()
    else:
        # fact_order_items: composite key
        pk_dups = df.duplicated(subset=["order_id", "item_id"]).sum()
        pk = "order_id + item_id"
    dup_data.append({
        "Table": name,
        "Primary Key": pk,
        "Full Row Duplicates": full_dups,
        "PK Duplicates": pk_dups,
        "Status": "✅ Clean" if pk_dups == 0 else "⚠️ Has Duplicates",
    })

dup_df = pd.DataFrame(dup_data)
print(dup_df.to_string(index=False))

# ============================================================
# 5. DATA TYPE VALIDATION
# ============================================================
print("\n" + "=" * 70)
print("TABLE 4: DATA TYPE CHECK")
print("=" * 70)

dtype_data = []
for name, df in datasets.items():
    for col in df.columns:
        dtype_data.append({
            "Table": name,
            "Column": col,
            "Current Dtype": str(df[col].dtype),
            "Sample Value": str(df[col].dropna().iloc[0]) if len(df[col].dropna()) > 0 else "N/A",
        })

dtype_df = pd.DataFrame(dtype_data)
print(dtype_df.to_string(index=False))

# ============================================================
# 6. STAR SCHEMA RELATIONSHIP VALIDATION
# ============================================================
print("\n" + "=" * 70)
print("TABLE 5: STAR SCHEMA RELATIONSHIP VALIDATION")
print("=" * 70)

relationships = [
    ("fact_orders", "customer_id", "dim_customer", "customer_id"),
    ("fact_orders", "restaurant_id", "dim_restaurant", "restaurant_id"),
    ("fact_orders", "delivery_partner_id", "dim_delivery_partner", "delivery_partner_id"),
    ("fact_orders", "order_id", "fact_delivery_performance", "order_id"),
    ("fact_orders", "order_id", "fact_ratings", "order_id"),
    ("fact_order_items", "order_id", "fact_orders", "order_id"),
    ("fact_order_items", "menu_item_id", "dim_menu_item", "menu_item_id"),
]

rel_data = []
for fact_table, fact_col, dim_table, dim_col in relationships:
    fact_df = datasets[fact_table]
    dim_df = datasets[dim_table]
    
    fact_keys = set(fact_df[fact_col].dropna().unique())
    dim_keys = set(dim_df[dim_col].dropna().unique())
    
    orphan_count = len(fact_keys - dim_keys)
    match_pct = round((1 - orphan_count / len(fact_keys)) * 100, 2) if len(fact_keys) > 0 else 100
    
    rel_data.append({
        "Fact Table": fact_table,
        "FK Column": fact_col,
        "Dim Table": dim_table,
        "PK Column": dim_col,
        "Orphan Keys": orphan_count,
        "Match %": match_pct,
        "Status": "✅ Valid" if orphan_count == 0 else f"⚠️ {orphan_count} orphans",
    })

rel_df = pd.DataFrame(rel_data)
print(rel_df.to_string(index=False))

# ============================================================
# 7. KEY STATISTICS & DISTRIBUTIONS
# ============================================================
print("\n" + "=" * 70)
print("TABLE 6: KEY FIELD DISTRIBUTIONS")
print("=" * 70)

# Orders date range
fact_orders["order_timestamp"] = pd.to_datetime(fact_orders["order_timestamp"])
print(f"\n📅 Order Date Range: {fact_orders['order_timestamp'].min()} to {fact_orders['order_timestamp'].max()}")

# Monthly order counts
monthly = fact_orders.groupby(fact_orders["order_timestamp"].dt.to_period("M")).size()
print(f"\n📊 Monthly Order Counts:")
for period, count in monthly.items():
    phase = "Pre-Crisis" if str(period) <= "2025-05" else "Crisis"
    print(f"   {period}: {count:,} orders [{phase}]")

# Cancellation check
cancel_counts = fact_orders["is_cancelled"].value_counts()
print(f"\n❌ Cancellation Distribution:")
for val, count in cancel_counts.items():
    print(f"   {val}: {count:,} ({round(count/len(fact_orders)*100, 2)}%)")

# COD check
cod_counts = fact_orders["is_cod"].value_counts()
print(f"\n💰 COD Distribution:")
for val, count in cod_counts.items():
    print(f"   {val}: {count:,} ({round(count/len(fact_orders)*100, 2)}%)")

# City distribution
print(f"\n🏙️ Cities: {sorted(dim_customer['city'].unique())}")
print(f"🍽️ Cuisine Types: {sorted(dim_restaurant['cuisine_type'].unique())}")
print(f"🏪 Partner Types: {sorted(dim_restaurant['partner_type'].unique())}")
print(f"🚗 Vehicle Types: {sorted(dim_delivery_partner['vehicle_type'].unique())}")
print(f"📢 Acquisition Channels: {sorted(dim_customer['acquisition_channel'].unique())}")

# Ratings range
print(f"\n⭐ Rating Range: {fact_ratings['rating'].min()} to {fact_ratings['rating'].max()}")
print(f"📝 Sentiment Score Range: {fact_ratings['sentiment_score'].min()} to {fact_ratings['sentiment_score'].max()}")
print(f"📝 Reviews Available: {fact_ratings['review_text'].notna().sum():,} / {len(fact_ratings):,}")

# ============================================================
# 8. NUMERIC FIELD STATISTICS
# ============================================================
print("\n" + "=" * 70)
print("TABLE 7: NUMERIC FIELD STATISTICS (fact_orders)")
print("=" * 70)

numeric_cols = ["subtotal_amount", "discount_amount", "delivery_fee", "total_amount"]
stats = fact_orders[numeric_cols].describe().round(2)
print(stats.to_string())

print("\n" + "=" * 70)
print("TABLE 8: DELIVERY PERFORMANCE STATISTICS")
print("=" * 70)

del_stats = fact_delivery[["actual_delivery_time_mins", "expected_delivery_time_mins", "distance_km"]].describe().round(2)
print(del_stats.to_string())

# ============================================================
# 9. DATA QUALITY SCORE
# ============================================================
print("\n" + "=" * 70)
print("📋 DATA QUALITY SUMMARY")
print("=" * 70)

total_nulls = sum(df.isnull().sum().sum() for df in datasets.values())
total_cells = sum(df.shape[0] * df.shape[1] for df in datasets.values())
completeness = round((1 - total_nulls / total_cells) * 100, 2)

total_pk_dups = sum(row["PK Duplicates"] for _, row in dup_df.iterrows())
total_orphans = sum(row["Orphan Keys"] for _, row in rel_df.iterrows())

print(f"\n   Data Completeness:      {completeness}%")
print(f"   Total Null Values:      {total_nulls:,}")
print(f"   PK Duplicates Found:    {total_pk_dups:,}")
print(f"   Orphan Keys Found:      {total_orphans:,}")
print(f"   Tables Validated:       {len(datasets)}/8")
print(f"   Relationships Checked:  {len(relationships)}/7")

quality_score = completeness  # Simplified; can weight further
if total_pk_dups > 0:
    quality_score -= 5
if total_orphans > 0:
    quality_score -= (total_orphans / 1000)

print(f"\n   🏆 OVERALL DATA QUALITY SCORE: {round(quality_score, 1)}%")
if quality_score >= 95:
    print("   ✅ GO/NO-GO: PROCEED TO ANALYSIS")
else:
    print("   ⚠️ GO/NO-GO: REVIEW ISSUES BEFORE PROCEEDING")

# ============================================================
# 10. EXPORT REPORT TO CSV
# ============================================================
overview_df.to_csv(f"{OUTPUT_DIR}01_dataset_overview.csv", index=False)
null_df.to_csv(f"{OUTPUT_DIR}02_null_analysis.csv", index=False)
dup_df.to_csv(f"{OUTPUT_DIR}03_duplicate_check.csv", index=False)
rel_df.to_csv(f"{OUTPUT_DIR}04_relationship_validation.csv", index=False)
dtype_df.to_csv(f"{OUTPUT_DIR}05_dtype_check.csv", index=False)

# Export full summary as markdown
with open(f"{OUTPUT_DIR}DATA_QUALITY_REPORT.md", "w") as f:
    f.write("# QuickBite Express - Data Quality Report\n\n")
    f.write(f"**Generated:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
    f.write(f"**Data Quality Score:** {round(quality_score, 1)}%\n\n")
    f.write("---\n\n")
    f.write("## Dataset Overview\n\n")
    f.write(overview_df.to_markdown(index=False))
    f.write("\n\n## Null Analysis\n\n")
    f.write(null_df.to_markdown(index=False))
    f.write("\n\n## Duplicate Check\n\n")
    f.write(dup_df.to_markdown(index=False))
    f.write("\n\n## Relationship Validation\n\n")
    f.write(rel_df.to_markdown(index=False))
    f.write("\n\n---\n")
    f.write(f"\n**Status:** {'PROCEED TO ANALYSIS' if quality_score >= 95 else 'REVIEW ISSUES'}\n")

print(f"\n📁 Reports exported to: {OUTPUT_DIR}")
print("   - 01_dataset_overview.csv")
print("   - 02_null_analysis.csv")
print("   - 03_duplicate_check.csv")
print("   - 04_relationship_validation.csv")
print("   - 05_dtype_check.csv")
print("   - DATA_QUALITY_REPORT.md")
print("\n✅ Sprint 1, Task 1 COMPLETE!")
