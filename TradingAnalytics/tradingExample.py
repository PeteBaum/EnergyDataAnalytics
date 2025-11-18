import polars as pl
from pathlib import Path

# import data
data_dir = Path("TradingAnalytics/Data/actual_production_per_plant")
csv_files = list(data_dir.glob("*.csv"))


# Import prices data
prices_df = pl.read_csv("TradingAnalytics/Data/prices/prices2025.csv", null_values=["n/e", ""])

# Add datetime column by parsing the first part of "MTU (CET/CEST)"
prices_df = prices_df.with_columns(
    pl.col("MTU (CET/CEST)")
    .str.split(" - ")
    .list.get(0)
    .str.to_datetime(format="%d/%m/%Y %H:%M:%S")
    .alias("datetime")
)

# Resample to hourly resolution using average
prices_df = prices_df.group_by_dynamic(
    "datetime",
    every="1h"
).agg([
    pl.col("Day-ahead Price (EUR/MWh)").mean().alias("day_ahead_price"),
]).sort("datetime")


# Read and combine all CSV files
plant_production_df = pl.concat([pl.read_csv(file, null_values=["n/e", ""]) for file in csv_files])

# Add datetime column by parsing the first part of "Time Interval (CET/CEST)"
plant_production_df = plant_production_df.with_columns(
    pl.col("MTU (CET/CEST)")
    .str.split(" - ")
    .list.get(0)
    .str.to_datetime(format="%d/%m/%Y %H:%M")
    .alias("datetime")
)

# Select and rename columns, fill null values in generation
plant_production_df = plant_production_df.select([
    pl.col("datetime"),
    pl.col("Unit Name").alias("unit"),
    pl.col("Generation (MW)").fill_null(0).alias("generation")
])

# Join day_ahead_price to plant_production_df
plant_production_df = plant_production_df.join(
    prices_df.select(["datetime", "day_ahead_price"]),
    on="datetime",
    how="left"
)

# Get the marginal prices for each power plant
results = []

for unit_name in plant_production_df["unit"].unique():
    # Filter and sort by datetime
    unit_df = plant_production_df.filter(pl.col("unit") == unit_name).sort("datetime")

    #Round the generation of the power plant to the next 50MW to ignore small ramps etc
    unit_df = unit_df.with_columns(
        (pl.col("generation") / 50).ceil().mul(50).alias("generation")
    )
    
    # Skip if all generation values are zero
    if unit_df["generation"].max() == 0:
        continue

    # If all generation values are non-zero, marginal price is 0
    if unit_df["generation"].min() > 0:
        results.append({
            "unit": unit_name,
            "generation": unit_df["generation"].max(),
            "marginal_price": 0
        })
        continue
    
    # Create groups for consecutive non-zero generation periods
    # Mark where generation changes from 0 to non-zero or vice versa
    unit_df = unit_df.with_columns(
        (pl.col("generation") == 0).alias("is_zero")
    )
    
    # Create group ID that changes whenever we transition between 0 and non-zero
    unit_df = unit_df.with_columns(
        pl.col("is_zero").cum_sum().alias("group_id")
    )
    
    # Calculate average day_ahead_price for each group
    group_avg = unit_df.group_by("group_id").agg([
        pl.col("day_ahead_price").mean().alias("avg_price")
    ])

    # Join group_avg back to unit_df
    unit_df = unit_df.join(group_avg, on="group_id", how="left")
    
    unit_df = unit_df.filter(pl.col("generation")==pl.col("generation").max())
    
    results.append({
            "unit": unit_name,
            "generation": unit_df["generation"].max(),
            "marginal_price": unit_df["avg_price"].min()
        })

# Convert results to dataframe
marginal_results_df = pl.DataFrame(results)

# Sort by marginal_price and add cumulated generation column
marginal_results_df = marginal_results_df.sort("marginal_price").with_columns(
    pl.col("generation").cum_sum().alias("cumulated_generation")
)

# Create a stacked bar chart
import matplotlib.pyplot as plt

fig, ax = plt.subplots(figsize=(12, 6))

# Create bars for each unit
for i in range(len(marginal_results_df)):
    row = marginal_results_df.row(i, named=True)
    # Calculate the starting position (previous cumulated generation)
    x_start = row["cumulated_generation"] - row["generation"]
    
    ax.barh(
        y=0,
        width=row["generation"],
        left=x_start,
        height=row["marginal_price"],
        label=row["unit"],
        edgecolor='black',
        linewidth=0.5
    )

ax.set_xlabel("Cumulated Generation (MW)")
ax.set_ylabel("Marginal Price (EUR/MWh)")
ax.set_title("Merit Order Curve - Marginal Price by Cumulated Generation")
ax.grid(True, alpha=0.3)
plt.tight_layout()
plt.show()

print("blop")