# tpcds to gcs
Cloud Run Job to populate a Google Cloud Storage bucket with the TPC-DS benchmark

[![Run on Google Cloud](https://deploy.cloud.run/button.svg)](https://deploy.cloud.run)

**After deployed generate the files with**
[YOUR_CLOUD_RUN_ENDPOINT]/run?bucket=[BUCKET_NAME]&scale=[SCALE_FACTOR]

SCALE_FACTOR is a number in GB expressing the total amount of data created, use 0.1 for 100MB


For value of scale greater than 0.5 you will run easily out of memory, consider to increase the "Memory Allocated" parameters.