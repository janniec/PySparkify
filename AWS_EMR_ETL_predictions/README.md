# Deployment on AWS  
This portion of the project is still a WORK IN PROGRESS. The files in this folder are drafts of jobs that I plan to deploy after I configure AWS.  
  
  
## Data Pipeline  
![PySpark Framework](https://github.com/janniec/PySparkify/blob/main/images/AWS_EMR.jpg)  
image source: [conicear.best](https://conicear.best/product_details/4405045.html)  
  
Sparkify has tasked me with deploying the ETL and modeling onto AWS,   
    - extracting data from S3,  
    - transforming it with Spark on an EMR cluster,  
    - training a model with SparkML,  
    - saving the model to S3, and  
    - making predictions & saving them to S3  
This will allow the company to identify users at risk of churn at scale.  
  
   
## File Descriptions   
- README.md - status and information about deployment  
- dl.cfg - configuration file with AWS access keys  
- etl.py - contains the ETL job that pull raw data from S3, transforms the data, and save it back into S3  
- create_model.py - contains the job to pull transformed data and labels from S3, train a model, and save the model into S3  
- predict.py - contains the job to pull the transformed data from S3, load the trained model, generate predictions, and save them into s3  
  
  
## Next Steps   
Schedule the jobs to run in periodic batches.   
1. Update the ETL job to transform data in daily batches.  
2. set up a model evaluation job, to be used as a condition to update the model.  
3. Update the Predict job to load the most recent model and only predict on daily batches.  
4. Set up an Airflow DAG to schedule the jobs.  