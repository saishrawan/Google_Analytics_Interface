# Google_Analytics_Interface

The Google Analytics Data Interface module retrieves event- and session-level metrics across multiple GA4 properties via the Google Analytics Data API, samples the results, and writes them into the Delta table pocn_data.silver.google_analytics_data_qa for downstream analysis.

[GA4 Data API] 
       ↓ (gRPC via google-analytics-data client, pagination)
[Python Script] 
       ↓ (RunReport → Pandas DataFrame per property)
       ↓ (sampling & Spark conversion)
[Union all Spark DataFrames]
       ↓ (write as Delta)
[Spark Table: pocn_data.silver.google_analytics_data_qa]
