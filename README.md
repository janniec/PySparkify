# Sparkify  
Sparkify is a digital music streaming service similar to Spotify or Pandora. Users can stream songs for free using the free tier that plays advertisements between songs or stream music without advertisements through the premium tier for a flat monthly fee. Users can upgrade, downgrade, or cancel their accounts at anytime. It is crucial for Sparkify's business to thrive that users love the service. If it could accurately identify users before they cancel, Sparkify could offer them incentives to stay.   
  
**Problem Statement:** Sparkify wants to be able to predict which users are at risk to churn. For this project, we will limit the definition of "churn" to only users who have confirmed the cancellation of their accounts.  The purpose of this exercise is to find the best classifier, the best parameters, and the best set of features to build a model to predict churn on the full dataset in a Spark Cluster on AWS. 
  
   
## Data  
Data for this project was provided by Udacity.  Every time a user interacts with the Sparkify service, whether playing songs, seeing ads, changing their tier, it generates event data. The full dataset is 12GB and available in a Spark Cluster on AWS. However during the investigative portion of this assignment we will be working locally with a 128MB subset.  
  
The data contains the following demographic values:  
- userId (string) - unique identifier for the user  
- firstName (string) - user's first name  
- lastName (string) - user's last name  
- gender (string) - user's gender (M or F)  
- location (string) - user's location  
- userAgent (string) - user's browser/operating system  
- registration (int) - epoch timestamp of user's account registration  
- level (string) - user's subscription label (free or paid)  
  
The data contains the following event-level values:  
- artist (string) - artist name for song  
- song (string) - song name  
- length (float) - length of song in seconds  
- ts (int) - epoch timestamp of the log entry  
- page (string) - type of interaction of log, e.g. NextSong, Cancel, Home  
- auth (string) - authentication level (Logged In, Logged Out, Cancelled, or Guest)  
- sessionId (int) - unique identifier for the session  
- itemInSession (int) - log count in the session  
- method (string) - HTTP request method (GET or PUT)  
- status (int) - HTTP status code, (200, 307, 404)   
  
  
## Libraries  
The libraries required for this code are listed in 'requirements.txt'. In order to install all the libraries: `pip3 install -r requirements.txt`.  
- PySpark  
- Pandas  
- Matplotlib  

![PySpark Framework](https://github.com/janniec/PySparkify/blob/main/images/internals-of-job-execution-in-apache-spark.jpg)  
image source: [data-flair.training](https://data-flair.training/blogs/dag-in-apache-spark/)   
  
## PySpark
  
PySpark is a distributed processing system that will distribute data across a cluster and process the data in parallel. In brief,  
**Driver Program (Master)** initializes the Spark Context, builds DAGs with the code, schedules job execution with the Cluster Manager, and distributes tasks to the Executors.  
**Spark Context** connects Spark Applicatin with the Spark Cluster, a distributed computing system where each node has its own memory and processors.  
**Resilient Distributed Dataset (RDD)** is a partitioned copy of the input data. All the partitions are stored across the cluster.   
**Directed Acyclic Graph (DAG)** are chains of tasks from the code that the DAG Scheduler holds as stages of tasks.  
**Cluster Manager** allocates and monitors the available resources and launches the Executors with tasks.  
**Executors (Workers)** perform the data processing across the nodes in parallel, stores the results in memory, returns the result to the Driver once the tasks have been completed.   
  
  
## File Descriptions  
- README.md - information about this repository  
- requirements.txt - requirements.txt: text of the python packages required to run this project  
- Sparkify_Exploratory_Analysis_Feature_Engineering.ipynb - notebook that explores the data and generates features     
- Sparkify_Modeling.ipynb - notebook that finds the best classifier, parameters, and features to predict churn among users  
- mini_sparkify_event_data.json - event data from the Sparkify service   
- blogpost.md - blog post about the findings from analysis in this repository  
- 25 images files in the 'images' directory: images for the blogpost
  
  
## Results  
I approached this project in 3 stages: (1) I quickly found the best default classifier, (2) I found the best parameters for this classifier, and (3) I found the most important features to train our model. 
  
Throughout experimentation, I produced 3 models to run on the validation data set:  
- Random Forest Classifier with default parameters trained on 85 features. F1 Score: 71%  
This model became our baseline for performance. 
- Random Forest Classifier with a better set of parameters trained on 85 features. F1 Score: 76%
The `impurity` and `maxBin` parameters mostly remained the same as the default values. But the model performed much better with  a higher `maxDepth`  than the default and much lower `numTrees` than the default, which combined indicated that the default model was underfit and a better model just needed to be closer fit to the training data.
- Random Forest Classifier with the same better set of parameters train on 4 features. F1 Score: 70% 
Because data processing took so long, I continued my experiments to include feature selection. Cross validation on various sets of features indicated that only 4 features were really necessary to predict user churn--`Thumbs Down`, `Log out`, `200` HTTP Status and `PUT` HTTP method request. Although training a model on just 4 features would sacrifice 6% in performance, it would speed up data process signficantly. 
  
Ultimately we improved our model by 5% and we found the most crucial features.
  
  
## Next Steps  
Productionalize this model and deploy it on the Spark cluster on the full dataset on AWS.  