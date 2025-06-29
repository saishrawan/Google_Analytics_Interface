from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import DateRange
from google.analytics.data_v1beta.types import Dimension
from google.analytics.data_v1beta.types import Metric
from google.analytics.data_v1beta.types import RunReportRequest
from google.oauth2 import service_account
import pandas as pd
import numpy as np
from datetime import date
from datetime import timedelta
import matplotlib.pyplot as plt
import seaborn as sns

property_ids = ["273585788", "273566672","425540994","369287665","295474161","434404281","419266345","422124039","305079035","417584750","433162389","335956542","418066855","330153555","431428607","418089883","280657549","435429820","335557691","340476082","299464401","369309761"]

# Path to your service account key file
key_path = "/dbfs/FileStore/Quickstart_ef6303b62d1f.json"

# Authenticate and create a client
credentials = service_account.Credentials.from_service_account_file(key_path)
client = BetaAnalyticsDataClient(credentials=credentials)

# Define dimensions and metrics
dimensions = [
    Dimension(name="eventName"),
    Dimension(name="pagePath"),
    Dimension(name="sessionMedium"),
    Dimension(name="sessionSource"),
    Dimension(name="sessionCampaignName"),
    Dimension(name="firstUserMedium"),
    Dimension(name="firstUserSource"),
    Dimension(name="firstUserCampaignName"),
    Dimension(name="pageLocation"),
    # Dimension(name="linkUrl")
]

metrics = [
    Metric(name="eventCount"),
    Metric(name="totalUsers"),
    Metric(name="newUsers"),
    Metric(name="sessions"),
    Metric(name="engagedSessions"),
    Metric(name="engagementRate")
]

property_ids = ["273585788", "273566672", "425540994","369287665","417584750"]

# Function to handle pagination and fetch all data
def fetch_all_data(property_id, dimensions, metrics):
    rows = []
    offset = 0
    limit = 10000  # Adjust based on API limits

    while True:
        request = RunReportRequest(
            property="properties/" + property_id,
            dimensions=dimensions,
            metrics=metrics,
            date_ranges=[DateRange(start_date=begin_date, end_date=ending_date)],
            limit=limit,
            offset=offset
        )
        response = client.run_report(request)
        if not response.rows:
            break
        for row in response.rows:
            row_data = {dim.name: row.dimension_values[i].value for i, dim in enumerate(dimensions)}
            row_data.update({met.name: row.metric_values[i].value for i, met in enumerate(metrics)})
            row_data["property_id"] = property_id  # Add property_id to row data
            row_data["date"] = begin_date 
            rows.append(row_data)
        offset += limit

    return pd.DataFrame(rows)

# Initialize an empty list to hold Spark dataframes
spark_dfs = []

# Initialize a list to hold record counts
record_counts = []

# Loop through each property and fetch all data
for property_id in property_ids:
    df = fetch_all_data(property_id, dimensions, metrics)
    record_counts.append((property_id, len(df)))  # Add record count for each property
    if not df.empty:
        # Sample 1000 rows from each DataFrame
        sample_df = df.sample(n=100, random_state=1) if len(df) > 1000 else df
        # Convert sampled Pandas DataFrame to Spark DataFrame
        spark_df = spark.createDataFrame(sample_df)
        spark_dfs.append(spark_df)
    else:
        print(f"No data fetched for property_id: {property_id}")

# Print the number of records for each property_id
for property_id, count in record_counts:
    print(f"Property ID: {property_id}, Number of Records: {count}")

# Concatenate all Spark dataframes
if spark_dfs:
    final_spark_df = spark_dfs[0]
    for df in spark_dfs[1:]:
        final_spark_df = final_spark_df.union(df)

    # Write the Spark DataFrame to a Databricks table
    final_spark_df.write.format("delta").mode("append").saveAsTable("pocn_data.silver.google_analytics_data_qa")

    # Optionally display the first few rows
    final_spark_df.show(10)

    # Display the distinct property_id values
    final_spark_df.select("property_id").distinct().show()

    # Convert the final Spark DataFrame to Pandas DataFrame
    final_pandas_df = final_spark_df.toPandas()

    # Save the Pandas DataFrame to CSV and XLSX formats
    csv_path = "/dbfs/FileStore/google_analytics_data_qa.csv"
    final_pandas_df.to_csv(csv_path, index=False)

    print(f"Data saved to {csv_path}")
else:
    print("No data fetched for any property.")
