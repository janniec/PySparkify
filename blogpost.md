# Sparkify: A Week in the Life of a Hypothetical Data Scientist  
## Predicting User Churn with PySpark  
   
![Sparkify](https://github.com/janniec/PySparkify/blob/main/images/sparkify_churn.png)  
image source: [Medium](https://medium.com/analytics-vidhya/sparkify-predicting-the-user-churn-using-apache-spark-ee4178f859c8)   
  
## Introduction  
  
Udacity asked me to imagine that I worked on the data team for a hypothetical popular digital music streaming service, like Spotify or Pandora, called Sparkify. NO PROBLEM. Users can stream songs for free using the free tier that plays advertisements between songs or stream music without advertisements through the premium tier for a flat monthly fee. Users can upgrade, downgrade, or cancel their accounts at anytime. It is crucial for Sparkify's business to thrive that users love the service.   
  
  
## Problem Statement (Monday 9AM)   
   
![Stand up meeting](https://github.com/janniec/PySparkify/blob/main/images/agile-stand-up-meeting.jpg)  
image source: [workfront.com](https://www.workfront.com/project-management/methodologies/agile/daily-stand-up)  
    
Monday morning at the daily stand up meeting, the Product Manager let's me know that she has project for me and she would like to meet after stand up to discuss details.   
**Problem Statement**:  Sparkify wants to be able to predict which users are at risk of churn. A user has churned only when the user's account cancellation is confirmed. If Sparkify could accurately identify users before they cancel, it could offer them incentives to stay.   
  
I ask for a full sprint, 2 weeks, but the Product Manager would like a progress update at the end of the week.   
   
   
## Review Data (Monday 10 AM)   
  
I go back to my desk and take a closer look at the event data Sparkify collects. (Data for this project was actually provided by Udacity.) Every time a user interacts with the Sparkify service, whether playing songs, seeing ads, changing their tier, it generates event data. The full dataset is 12GB and available in a Spark Cluster on AWS. However during the investigative portion of this assignment I will be working locally with a 128MB subset.   
   
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
  
  
## Scope Project (Monday 11 AM)   
   
**Approach:** I plan to use this week to find the fastest and most accurate model to run on the full dataset on a Spark cluster on AWS next week.  I expect that the number of churned users is far fewer than the number of users who stayed. So given the imbalance, I will be evaluating the models with f1 scores. In addition, I expect that data transformation required to generate features will be time consuming. I will attempt to find the fewest set of most important features necessary to predict which users will churn without sacrificing too much of the f1 score.   
  
**Libraries**   
For this project, I expect to use the following tools:   
- PySpark   
- Pandas   
- Matplotlib  
  
**Why use PySpark?**  
12GB is a substantial amount of data and while determining which users are at risk may not need to be a real-time process, Sparkify probably will want to run this model regularly with preferably fast computation. For these reasons, I would recommend that we use a distributed processing system that will distribute data across a cluster and process the data in parallel.  
   
   
## Data Preprocessing (Monday after Lunch)  
   
![Working through Lunch, Lets be Honest.](https://github.com/janniec/PySparkify/blob/main/images/lunch-at-my-desk.jpeg)  
image source: [huffpost.com](https://www.huffpost.com/entry/why-you-should-never-eat-lunch-at-your-desk_l_60522125c5b6ce101643c3f8)  
   
During my preliminary exploration of the data, I found that there were data points that could not be attributed to any users because the `userId`s were empty strings. Those log entries can be cleaned out, as well as duplicates.  
    
To make exploratory analysis more digestible, I can already see that 4 data points should be transformed and simplified:  
- `location`: There are 114 different city for 225 unique users. Instead, lets investigate the users in aggregations by state.   
- `ts` and `registration`: are in epoch form. Converting them to date times and focusing on just the dates maybe give us more insights than timestamps.  
- `useragent`: This column appears to track the operating system used to access the Sparkify service. Preliminary review of the values appear to show near duplications and overlap. So I will simplify this column by focusing on the operating systems brands. I should note that there appears to be a hybrid type of operating system that is "compatible" with multiple brands. For this project, I will make 'compatible' a separate category from 'Macintosh', 'Windows', and 'Linux'.  
  
But before I leave for the day, I need to create label that we want our model to predict. Sparkify defined "churn" as only when the user's account cancellation is confirmed.  So I will label `userId`s that have a log entry where the `page` value is `Cancellation Confirmation`.  
  
  
## Exploratory Analysis (Tuesday)   
   
![Working on Visualizations](https://github.com/janniec/PySparkify/blob/main/images/working-on-the-desk.jpg)  
image source: [osmondmarketing.com](https://www.osmondmarketing.com/why-a-well-organized-office-is-beneficial-to-your-work)   
   
To understand how the data I have could help me predict users who are at risk of churn, I looked at the difference between users who stayed and users who churned, the number of users who churned or stayed across the different events, how the users in the two different groups behaved, what events they created, and how that behavior changed over time.  I did a fairly comprehensive analysis, but here are the most interesting findings in my exploration.  
   
### Number of Users in Each Group across Categories   
   
![Count of Users who Churned vs Stayed](https://github.com/janniec/PySparkify/blob/main/images/count_of_users_churned_stayed.png)  
  
We are working with 225 users in the subset. As expected, users who churned only make about 23% of all the users in our data subset. This confirms that I will be attempting to predict an imbalanced class.   
  
![Count of Users who Churned vs Stayed Over Time](https://github.com/janniec/PySparkify/blob/main/images/count_of_churned_over_time.png)  
  
There appears to be a decline in the number of users who churn. But we don't have enough of a window to tell if this is cyclical. This is something to investigate with a bigger set of data next week.  
   
![Count of Users who Churned vs Stayed and Paid vs Free](https://github.com/janniec/PySparkify/blob/main/images/count_of_users_churned_paid_free.png)  
  
I was surprised that there are more people who churned within the free tier, given that the service wasn't costing them. But because Sparkify allows their users to downgrade and upgrade at will, this visualization only represents a snapshot in time.   
   
![Count of Users who Churned vs Stayed and Paid vs Free Over Time](https://github.com/janniec/PySparkify/blob/main/images/count_of_users_churned_paid_free_over_time.png)  
  
As expected, the same data visualized over time was more enlightening. Interestingly, users who churned, regardless of tier, and users who stayed on the free tier are roughly similar. There are significantly more users who stayed on the paid tier, indicating that users maybe be more likely to stay if they are on paid tier.   
  
  
### Number of Log Events across Categories for Each Group    
    
![Count of Page Events for Users who Churned vs Stayed](https://github.com/janniec/PySparkify/blob/main/images/count_of_page_events.png)    
   
The count of `NextSong` log entries within the `page` column is skewing the visualization. This is unsurprising given that the main point of the service is to stream songs. However, I removed it from the visualization to see how the other `page` values broke down.  
   
![Count of Page Events for Users who Churned vs Stayed without NextSong](https://github.com/janniec/PySparkify/blob/main/images/count_of_page_events_wo_nextsong.png)   
   
Without `NextSong` entries, I could see that `Cancellation Confirmation` and `Cancel` are still rare occurances. Interestingly, account `Downgrade`s are more associated with users who stayed than users who churned, indicating that those who churned may have either stayed in the free tier, therefore having no reason to downgrade their account, or churned while in the paid tier. In addition, it appears that none of the user who churned encountered the `error` page.  
   
![Count of Status Values for Users who Churned vs Stayed](https://github.com/janniec/PySparkify/blob/main/images/count_of_status_values.png)     
`status` refers to the HTTP status codes of Sparkify service at the time of the log creation. `200` means that internet is connecting and I'm not surprised that it is the most frequent value and therefore skews the visualization. Lets rerun it without `200`.  
   
![Count of Status Values for Users who Churned vs Stayed without 404](https://github.com/janniec/PySparkify/blob/main/images/count_of_status_values_wo_404.png)  
   
`307` indicates that a page was redirected and `404` indicates an error. Unexpectedly, it doesn't appear that users who churned were those who encountered more service disruptions.  
   
Overall, it appears that users who churned did not cancel the services because of internet disruptions or errors. At this point in the project, I have a theory that users are leaving because of the content - the songs within the service.  
  
  
## Feature Engineering (Wednesday)   
   
![Chill Day at Work](https://github.com/janniec/PySparkify/blob/main/images/relaxed-calm-work.jpg)  
image source: [dreamstime.com](https://www.dreamstime.com/photos-images/relief-peace.html)  
  
I am anticipating that the data processing required to generate features will be the most time consuming aspect of this project. (Once I write the code to generate the features, I can relax and just troubleshoot as needed.) And we should aim to minimize this step in the future for the full data set. As such, I plan to be more comprehensive with the features generate for the subset, and incorporate feature selection as part of the modeling experiments. 
  
I generated the following 85 features:  
- Label for users who churned vs stayed  
- Count features  
    - Count of sessions per user  
    - Count of songs per user  
    - Count of Logs per user  
    - Count of artist per user  
    - Count of logs on free tier per user  
    - Count of logs on paid tier per user  
    - Count of days on free tier per user  
    - Count of days on paid tier per user  
    - Value counts of each HTTP method per user  
    - Value counts of each HTTP status per user  
    - Value counts of each page per user  
    The `Cancellation Confirmation` page is the label that we are predicting and I removed it as a feature. However, users would not see the `Cancellation Confirmation` page without seeing the `Cancel` page first, so `Cancel` should also be removed.  
- Time features  
    - Total session duration per user  
    - Average session duration per user  
    - Total song length per user  
    - Average song length per session  
- Dummy categorical features  
The categorical variables need to be converted because the models only take numerical values. In addition, one value from each set of categories will need to be dropped to avoid multicollinearity in the models, especially logistic regression. In brief, mutlicollinearity among the features will lead to unstable estimates of the regression coefficiencts, which would make any feature importance calculations unreliable.      
    - gender (drop 'F')  
    - last level (dropping 'free')  
    - browser agent (dropped 'compatible')  
    - state (all 50 states are not present here)  
    I generated all 44 states just in case. I'm not sure if there will be a multicollinarity problem here because all 50 states aren't present in the subset of the data. Multicollinarity won't be an issue for tree based models as they would just drop duplicative features. However, if we proceed with a regression based model, I will review 2 versions of the model, one with all 44 states, one with only 43 states, and see if there is a change in performance.  
    
![Multicollinearity](https://github.com/janniec/PySparkify/blob/main/images/multicollinearity_problem.png)   
image source: [analyticsvidhya.com](https://medium.com/analytics-vidhya/multicollinearity-a-beginners-guide-fea13d01a171/)     
    
As expected, feature generation took several hours. So I saved the features dataset to simply load tomorrow.  
  
  
## Modeling (Thursday)  
   
![Fun at Work](https://github.com/janniec/PySparkify/blob/main/images/fun-at-work.jpg)  
image source: [stackoverflow.blog](https://stackoverflow.blog/2020/02/27/the-eight-factors-of-happiness-for-developers/)    
   
We're finally at the meat of this project, so to speak.  I approached this project iteratively - quickly creating a deliverable and improving upon it in stages.  I didn't know exactly how long modeling would take and I wanted to make sure that I would have something to hand over to the Product Manager.  In addition, I wanted to give Sparkify a few models to consider.  
   
Yesterday, I generated 85 features for 225 users. I split our data into sets:  
train data:  (117, 85)   
test data:  (41, 85)  
validation data:  (67, 85)  
   
### Which evaluation should we use? F1 Score  
   
Whether a user has churned or stayed is an imbalanced class. Only 23% of users in our dataset churned. As such, I will be evaluating the models with f1 scores, which is the harmonic mean of precision, which evaluates true positives against false positives, and recall, which evaluates true positives against false negatives. This means that f1 score will be a more class balanced evaluation measure.  
   
![Evaluation Scores](https://github.com/janniec/PySparkify/blob/main/images/evaluation.jpg)  
image source: [packtpub.com](https://subscription.packtpub.com/book/big_data_and_business_intelligence/9781785282287/10/ch10lvl1sec133/computing-precision-recall-and-f1-score)    
   
However, given that Sparkify is considering providing incentives to users who are at risk of churn, there may be costs associated with false positives-- mistaking content users for those who are at risk and given them unnecessary incentives to stay. So while I want to focus on f1 scores, I don't want to overlook accuracy completely.   
   
### Iteration 1: Find the best default classifier   
   
In order to predict on the full 12GB dataset in the Spark cluster on AWS, we will need the fastest, most accurate model. In addition, we will need the ability to review feature coefficients to pare down the number of features. The data transformation required to generate the 83 features took several hours when run locally. Because the longer we use the Spark cluster in AWS the more expensive it will be, I would like to perform the fewest number of data transformations on the full dataset without sacrificing too much accuracy. For these reasons, I looked at 4 possible classifiers:   
- Logistic Regression   
- Random Forest Classifier  
- Gradient Boosted Trees   
- Support Vector Machines   
   
For each classifer, I used default parameters, trained the classifiers on the same train dataset, tested the classifier on the same test dataset. I looked at each models' f1 score as well as speed:   
- **Logistic Regression**    
    - Training the model took 6.314727783203125 seconds.  
    - Accuracy: 0.6585365853658537  
    - F1 Score: 0.6354051927616049  
- **Random Forest Classifier**   
    - Training the model took 1.8004083633422852 seconds.  
    - Accuracy: 0.6829268292682927  
    - F1 Score: 0.574054436196536  
- **Gradient Boosted Trees**   
    - Training the model took 19.85404109954834 seconds.  
    - Accuracy: 0.5609756097560976  
    - F1 Score: 0.5609756097560975  
- **Support Vector Machines**  
    - Training the model took 26.991944074630737 seconds.  
    - Accuracy: 0.6097560975609756  
    - F1 Score: 0.5833202202989772   
    
As expected all the classifiers didn't do particularly well when using default parameters. While logistic regression had a slightly better f1 score, the random forest classifier had the better accuracy. Significantly, the random forest classifer was by far the fastest classifer. Given that I anticipate performance to improve with any of the classifier, and we are building a model that may need to run regularly on large data sets, I will proceed with the fastest classifier - random forest.   
   
### Iteration 2: Find the best parameters for our classifier    
   
[Documentation for Pyspark's Random Forest Classifier](https://spark.apache.org/docs/3.1.1/api/python/reference/api/pyspark.ml.classification.RandomForestClassifier.html) shows that the classifier has the following default parameters:  
- maxBins=32  
- maxDepth=5  
- numTrees=20  
- impurity='gini'  
   
To find better parameters, I explored `maxDepth`, `maxBins`, and `numTrees` values both lower than and higher than the defaults in the Parameter Grid. I also considered `entropy` as an alternative `impurity`, the metric used to calculate information gain at each node.  
   
![Parameter Grid and Cross Validation](https://github.com/janniec/PySparkify/blob/main/images/v2_paramgrid_crossv.png)  
   
In addition to Parameter Grid, I also used Cross Validation. The random forest algorithm has 2 random components when training models: (1) each tree trains on a random sample of the data and (2) each tree trains on a random subset of the features. A single experiment could result in a lucky jump in the f1 score. Instead, I will use a cross validator to split the train dataset into 3 folds, train a model on 2 folds and test on the third, rotate the folds and repeat twice more. Ultimately, for each combination of parameters, 3 models will be trained and evaluated, and we will get an average score. This will ensure that we get a combination of parameters that actually improved the model.  
   
![Random Forest Classification](https://github.com/janniec/PySparkify/blob/main/images/random_forest.png)  
image source: [towardsdatascience.com](https://towardsdatascience.com/the-ultimate-guide-to-adaboost-random-forests-and-xgboost-7f9327061c4f)    
    
The best parameters are gini with 30 maxBins, 10 maxDepth, and 5 trees.   
   
![Parameter Grid Results](https://github.com/janniec/PySparkify/blob/main/images/parameter_grid.png)  
   
The best parameters for our Random Forest Classifier are as follows:  
- `maxBins`: 30 (the default was 32)   
MaxBins is a parameter found in the Pyspark's Random Forest Classfier, but not in SciKit Learn's counterpart. PySpark's documentation claims that the number of bins are used to discretize continuous features. Increasing this value allows to algorithm to make more fine grained splits. I believe that increasing this number too high could lead to overfitting on the training data, while lower values could lead to under fitting. So in the parameter grid, I considered a significantly lower number of bins than the default, 10, and a slightly lower number of bins than the default, 30. The top performing models appear to gravitate towards a lower number of bins.  
- `maxDepth`: 10 (the default was 5)   
This parameter determines the size of the individual trees. A larger number here, could lead to over fitting. However, with multiple trees in the algorithm, this risk is lower.  
- `numTrees`: 5 (the default was 20)  
This parameter determines the number of trees. A lower number here, could lead to over fitting. The combination of a higher maxDepth along with a lower numTrees indicates that the default model was too underfitting.  
- `impurity`: `gini` (the default was 'gini')  
This parameter determines how trees split data a branch. In short, `gini` measures the probablity of a random sample of data points being classified incorrectly at a branch/node. `entropy` measures the impurity of information (the labels being one class is considered pure) at a branch. All other parameters being equal, switching this parameter to `entropy` would have dropped the f1 score by about 10%.  
   
### Iteration 3: Select the most important features to use in our best classifier   
   
When using the Spark cluster on AWS, the longer we need to run the code, the more it will cost us. The data transformation, required to create the features took a few hours when run locally. So Sparkify may want to limit the number of features. As such, I will look at the top important features and rerun our best classifier with the best parameters on fewer features.  Ideally, I will find a smaller set of features to run in the model without sacrificing too much of the f1 score.   
   
![Feature Importance](https://github.com/janniec/PySparkify/blob/main/images/feature_importance.png)   
   
Feature Importance shows that 41 out of the 85 features were used to train the second model. Just looking at the top few, high values in `Thumb Down` and `Logout` would strongly indicate disatisfaction with the service. Conversely, `200`, HTTP status code that the service is working, `PUT` HTTP method request to store data, `avg_song_length` and `NextSong` indicate engagement with the service. If those values were high, they would signal a user's satisfaction with the service. Whereas, low numbers would signal a user's disatisfaction.  
  
I experimented with various feature sets to see how the model performed with fewer features. This meant that for each set of features, I needed to split, vectorize and scale the data sets again before I train the models. In order to speed this up, I used PySpark's Pipeline for preprocessing and Cross validation for a more reliable evaluation of the models.  
  
![Pipeline and Cross Validation](https://github.com/janniec/PySparkify/blob/main/images/v3_pipeline_crossv.png)  
  
Cross validation on the various sets of features showed that only 4 features are really necessary to predict user Churn.   
The most important features are:  `['page_count_Thumbs_Down', 'page_count_Logout', 'status_count_200', 'method_count_PUT']`  
  
![Feature Selection](https://github.com/janniec/PySparkify/blob/main/images/feature_selection.png)  
  
  
## Validation (Friday Morning)  
  
![Almost Done](https://github.com/janniec/PySparkify/blob/main/images/almost_done.jpg)  
image source: [brinknews.com](https://www.brinknews.com/what-companies-need-to-do-to-improve-working-conditions-for-women/)   
   
We're almost at the finish line. I've built 3 models thus far. It's time to evaluate all 3 models on the validation data set and see if I improved the models with the iterations.  
   
**Model 1: Random Forest Model with Default Parameters**   
- Training the model took 2.3147828578948975 seconds.   
- Accuracy: 0.7910447761194029  
- F1 Score: 0.7119402985074627   
Surprisingly, the default random forest classifier performed pretty well on the validation set.  
  
**Model 2: Random Forest Model with the Best set of Parameters**   
- Training the model took 2.5150463581085205 seconds.  
- Accuracy: 0.7910447761194029  
- F1 Score: 0.7652003142183817   
With updated parameters but on the same train and validation datasets, the F1 score improved by 5%. Coincidentially, the accuracy is exactly the same. This indicates that the second model is better a predicting churned users but is offsetting the accuracy by misidentifying users who would have stayed as at risk of churn. In addition, the time to train the second model barely increased by .2 seconds.  
   
**Model 3: Best Random Forest Model on Selected Important Features**  
- Training the model took 2.378504514694214 seconds.  
- Accuracy: 0.7164179104477612  
- F1 Score: 0.7017556167458828   
Unsurprisingly, our 3rd model didn't perform as well. The F1 score dropped by 6% from the second model, 1% from the default model. Accuracy dropped about 8% from both models. The time to train the third model dropped to about the time it took to train the default model. Despite, this dip in performance, this third model is a model worth consideration. It only requires 4 features, which would probably take less than an hour of data processing when run locally!  
   
   
## Findings (Friday Afternoon)   
   
![Presenting my Findings](https://github.com/janniec/PySparkify/blob/main/images/woman-presenting.png)  
image source: [peopleelement.com](https://peopleelement.com/people-element-platform/meeting-with-woman-presenting/)    
   
### Summary   
   
Sparkify asked us to predict which users are at risk of churn, cancelling the service, so that they can incentivice those users to stay. The raw data was event-level log data with timestamps that had to be converted, `location` and `useragent` data that had to be simplified. The logs had to be aggregated into user-level data. Data exploration revealed that interestingly users who were on the paid tier were more likely to stay.  
   
Because this was a preliminary experiment, I was comprehensive in my feature generation, which took a few hours to create 85 features. It became apparent that such comprehensive feature generation could be exorbitantly time consuming and costly to perform on the full data set. My analysis was performed on a mere 128MB subset of a 12GB data set. So I mainly considered classifier models that would allow me to rank feature importance. I evaluated the model performances with f1 score because I was predicting an imbalanced class--only 23% of users churned. The default versions of the classifier performed similarly, but I chose to continue with the Random Forest Classifier because it was signficantly faster and we need a model that may need to run regularly on large datasets.  
  
### Improvement  
  
Through experimentation, I produced 3 models to run on our validation data set:   
- Random Forest Classifier with default parameters trained on 85 features. F1 Score: 71%   
This model became our baseline for performance.  
- Random Forest Classifier with a better set of parameters trained on 85 features. F1 Score: 76%  
The `impurity` and `maxBin` parameters mostly remained the same as the default values. But the model performed much better with  a higher `maxDepth`  than the default and much lower `numTrees` than the default, which combined indicated that the default model was underfit and a better model just needed to be closer fit to the training data.  
- Random Forest Classifier with the same set of parameters train on only 4 features. F1 Score: 70%  
Because data processing took so long, I continued my experiments to include feature selection. Cross validation on various sets of features indicated that only 4 features were really necessary to predict user churn--`Thumbs Down`, `Log out`, `200` HTTP Status and `PUT` HTTP method request. Although training a model on just 4 features would sacrifice 6% in performance, it would speed up data process signficantly. 
   
Ultimately I improved the model by 5% and I found the most crucial features. I expect this model would improve even further when it is trained on a much larger set of the data.  
   
## Next Steps (Next Week)   
  
If the Product Managers tells me that speed is of the essense, I will continue with the third model.  However, if she prefers a model with more accuracy, I will continue with second model and deploy it on the full dataset on the AWS cluster next week.  
  
  
### Reflection   
    
Although Sparkify asked us to predict users who were at risk of churning, its ultimate goal is to encourage users to stay. Exploratory Data Analysis indicated that user who churned weren't encountering `Error` pages or `404` status issues.  Feature importance showed us that the top feature influencing the models was `Thumbs Down`, meaning that a high occurence of this event in a user's account indicates dissatisfaction with the songs. While Sparkify said that they were considering providing discounts and incentives for users to stay, might I recommend that Sparkify improves its song recommendation engine. Perhaps the better approach to convincing users to stay is preventing users from streaming songs they would `Thumbs Down`. But that's next week's problem!   
  
![Cheers to the Weekend](https://github.com/janniec/PySparkify/blob/main/images/Happy-Hour.jpg)  
image source: [rokaakor.com](https://www.rokaakor.com/history-of-happy-hour-an-insider-look-into-this-post-work-tradition/)   
  
  
To see more about this analysis, please check out my [Github](https://github.com/janniec/Sparkify/blob/main/README.md).  