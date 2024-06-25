import configparser
import os
import datetime
from pyspark.sql import SparkSession
from pyspark.sql import functions
from pyspark.sql.functions import udf, avg, sum, min, max, count, col, filter, desc, when, dense_rank
from pyspark.sql.types import IntegerType
from pyspark.sql.window import Window


config = configparser.ConfigParser()
config.read('dl.cfg', encoding='utf-8-sig')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']

INPUT_DATA = 's3n://udacity-dsnd/sparkify/mini_sparkify_event_data.json' # mini dataset
# INPUT_DATA = 's3n://udacity-dsnd/sparkify/sparkify_event_data.json' # full dataset
OUTPUT_DIRECTORY = "s3a://sparkify/"


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def match_os_brand(useragent):
    '''
    useragent [string] = single value from the 'userAgent' column 
    Function to categorize the values in the 'userAgent' column
    os_brand [string] = 'compatible', 'Macintosh', 'Windows', or 'Linux'
    '''
    if 'compatible' in useragent:
        os_brand = 'compatible'
    elif 'Mac' in useragent:
        os_brand = 'Macintosh'
    elif 'Windows' in useragent:
        os_brand = 'Windows'
    else:
        os_brand = 'Linux'
    return os_brand

def clean_data(df):
    '''
    df [pyspark dataframe] = raw dataframe
    Function to clean dataframe
        - fill nulls
        - remove black userIds, duplicates
        - extract states, dates, and operating system brands
    df [pyspark dataframe] = cleaned dataframe
    '''
    # Delete rows with null `userId` or null `sessionId`
    df = df.dropna(how = 'any', subset = ['userId', 'sessionId'])

    # Delete rows where `userId` is `''`
    df = df.filter(df['userId'] != '')

    # Drop duplicate rows
    df = df.dropDuplicates()
    
    # Extract States from Locations
    extract_state = udf(lambda x: x.split(', ')[-1])
    df = df.withColumn('state', extract_state('location'))    
    
    # Extract date from `registration` and `ts`
    convert_date = udf(lambda x: datetime.datetime.fromtimestamp(x / 1000.0) \
                        .date().strftime('%Y-%m-%d'))
    df = df.withColumn('ts_date', convert_date('ts'))
    df = df.withColumn('registration_date', convert_date('registration'))
    
    # Extract Operating System brand 
    extract_os_brand = udf(lambda x: match_os_brand(x))
    df = df.withColumn('os_brand', extract_os_brand('userAgent'))
    return df

def count_per_user(df, column, newcolumn):
    '''
    df [pyspark dataframe] = cleaned dataframe
    column [string] = name of column we want to count
    newcolumn [string] = new column name for the count for each user
    Function to count unique values in a column for each user into a new dataframe
    unique_counts [pyspark dataframe] = dataframe of userIds and count
    '''
    col_df = df.select('userId', column).dropDuplicates()
    unique_counts = col_df.groupby('userId').count().withColumnRenamed('count', newcolumn)
    return unique_counts

def count_unique_features(df):
    '''
    df [pyspark dataframe] = cleaned dataframe 
    Function to create a dataframe of new columns that count unique values for each userId
    features [pyspark dataframe] = dataframe with userIds and new columns
    '''
    # Count of session
    f_session_count = count_per_user(df, 'sessionId', 'session_count')
    # Count of songs
    f_song_count = count_per_user(df, 'song', 'song_count')
    # Count of logs
    f_event_count = count_per_user(df, 'ts', 'log_count')
    # Count of artists
    f_artist_count = count_per_user(df, 'artist', 'artist_count')    
    
    features = f_session_count.join(f_artist_count,'userId','outer') \
                .join(f_song_count,'userId','outer') \
                .join(f_event_count,'userId','outer') \
                .dropDuplicates() \
                .fillna(0)    
    return features

def count_level(df, what_level, what_count, newcolumn):
    '''
    df [pyspark dataframe] = cleaned dataframe
    what_level [string] = 'free' or 'paid'
    what_count [string] = column name of unique values to count, dates or timestamps
    newcolumn ['string'] = new column name for the count for each user
    Function to count the number of logs or days each user was on the 'free' or 'paid' level
    level_counts [pyspark dataframe] = data frame of userIds and count
    '''
    col_df = df.where(df.level == what_level).select('userId', what_count).dropDuplicates()
    level_counts = col_df.groupby('userId').agg(count(what_count).alias(newcolumn))
    return level_counts

def count_level_features(df): 
    '''
    df [pyspark dataframe] = cleaned dataframe 
    Function to create a dataframe of new columns that count the number of logs or days
        on the 'free' or 'paid' level for each userId
    features [pyspark dataframe] = dataframe with userIds and new columns
    '''
    # Count of logs as a free user
    f_count_free_logs = count_level(df, 'free', 'ts', 'level_count_free_logs')   
    # Count of days as free user
    f_count_free_days = count_level(df, 'free', 'ts_date', 'level_count_free_days')
    # Count of logs as a paid user
    f_count_paid_logs = count_level(df, 'paid', 'ts', 'level_count_paid_logs')
    # Count of days as a paid user
    f_count_paid_days = count_level(df, 'paid', 'ts_date', 'level_count_paid_days')
    
    features = f_count_free_logs.join(f_count_free_days,'userId','outer') \
                .join(f_count_paid_logs,'userId','outer') \
                .join(f_count_paid_days,'userId','outer') \
                .dropDuplicates() \
                .fillna(0)    
    return features  

def occurence_count_pivot(df, column):
    '''
    df [pyspark dataframe] = cleaned dataframe 
    column [string] = name of column to count values
    Function to count the occurence of each value in the column for each user
    value_counts [pyspark dataframe] = dataframe of userIds and count of occurence for every value
    '''
    col_df = df.groupby('userId', column).count()
    value_counts = col_df.withColumn('pivot_col' \
                                     , functions.concat(functions.lit(column+'_count_') \
                                                        , col_df[column])) \
                     .groupby('userId').pivot('pivot_col').agg(functions.first('count')).fillna(0)
    return value_counts

def count_occurence_features(df): 
    '''
    df [pyspark dataframe] = cleaned dataframe 
    Function to create a dataframe of unqiue values from a column
        containing the count of the occurrence of that value for each userId
    features [pyspark dataframe] = dataframe with userIds and new columns
    '''
    # Count of each method per user
    f_method_counts = occurence_count_pivot(df, 'method')
    # Count of each status per user
    f_status_counts = occurence_count_pivot(df, 'status')
    # Count of each page per user
    f_page_counts = occurence_count_pivot(df, 'page') \
        .drop('page_count_Cancellation Confirmation') \
        .drop('page_count_Cancel')

    features = f_method_counts.join(f_status_counts,'userId','outer') \
                .join(f_page_counts,'userId','outer') \
                .dropDuplicates() \
                .fillna(0)    
    return features 

def get_duration_features(df): 
    '''
    df [pyspark dataframe] = cleaned dataframe 
    Function to create a dataframe of new columns for 
        the total and average duration of sessions and songs
    features [pyspark dataframe] = dataframe with userIds and new columns
    '''
    # Total session duration per user
    f_total_session_duration = df \
        .groupby('userId', 'sessionId') \
        .agg(((max('ts') - min('ts'))/1000).alias('session_length')) \
        .groupby('userId') \
        .agg(sum('session_length').alias('total_session_length')) 
    # Average session duration
    f_avg_session_duration = df \
        .groupby('userId', 'sessionId') \
        .agg(((max('ts') - min('ts'))/1000).alias('session_length')) \
        .groupby('userId') \
        .agg(avg('session_length').alias('avg_session_length'))

    # Total song length per user
    f_total_song_length = df\
        .where(df.page == 'NextSong')\
        .select('userId', 'length')\
        .groupBy('userId') \
        .sum() \
        .withColumnRenamed('sum(length)', 'total_song_length')

    # Average song length per session
    f_avg_song_length = df\
        .where(df.page == 'NextSong')\
        .groupby('userId', 'sessionId') \
        .agg(sum('length').alias('session_song_length'))\
        .groupby('userId') \
        .agg(avg('session_song_length').alias('avg_song_length'))
    
    features = f_total_session_duration.join(f_avg_session_duration,'userId','outer') \
                .join(f_total_song_length,'userId','outer') \
                .join(f_avg_song_length,'userId','outer') \
                .dropDuplicates() \
                .fillna(0) 
    return features

def dummy_cat_features(df):
    '''
    df [pyspark dataframe] = cleaned dataframe 
    Function to create a dataframe of new columns that are 1, 0 booleans of categorical values
    features [pyspark dataframe] = dataframe with userIds and new columns
    '''
    ### gender (dropping 'F')
    col_df = df.select('userId', 'gender').dropDuplicates()
    f_gender = col_df.replace({'M':'1', 'F':'0'}, subset='gender') \
        .withColumnRenamed('gender', 'gender_male')
    f_gender = f_gender.withColumn('gender_male', f_gender['gender_male'].cast(IntegerType()))

    ### last level (dropping 'free')
    time_window = Window.partitionBy('userId').orderBy(desc('ts'))
    df=df.withColumn('latest_time_rank', dense_rank().over(time_window))
    col_df = df.filter(df.latest_time_rank == 1).select('userId', 'level').dropDuplicates()
    f_last_level = col_df.replace({'paid':'1', 'free':'0'}, subset='level') \
        .withColumnRenamed('level', 'lastlevel_paid')
    f_last_level = f_last_level.withColumn('lastlevel_paid' \
                                           , f_last_level['lastlevel_paid'].cast(IntegerType()))

    ### state (all 50 states are not present here)
    categories = list(set([state for states in \
                           [states.state.split('-') for states in \
                            df.select('state').distinct().collect()] for state in states]))
    col_df = df.select('userId' ,'state').dropDuplicates()
    dummy_states = [functions.when(functions.col('state').contains(category), 1) \
                    .otherwise(0).alias('state_'+category) for category in categories]
    f_states = col_df.select('userId', *dummy_states)

    ### os brand (dropped 'compatible')
    categories = df.select('os_brand').distinct().rdd.flatMap(lambda x: x).collect()
    col_df = df.select('userId', 'os_brand').dropDuplicates()
    dummy_os = [functions.when(functions.col('os_brand')==category, 1) \
                .otherwise(0).alias('os_'+category) for category in categories]
    f_os_sys = col_df.select('userId', *dummy_os).drop('os_compatible')

    features = f_gender.join(f_last_level,'userId','outer') \
                .join(f_states,'userId','outer') \
                .join(f_os_sys,'userId','outer') \
                .dropDuplicates() \
                .fillna(0) 
    return features

def label_churn(df):
    '''
    df [pyspark dataframe] = cleaned dataframe
    Function to add columns to identify churn events and churned users
    df [pyspark dataframe] = labeled cleaned dataframe 
    '''
    # Create 'Churn' label
    boolean_churn = udf(lambda x: 1 if x == 'Cancellation Confirmation' else 0, IntegerType())
    df = df.withColumn('churn_event', boolean_churn('page'))
    
    # label `userId` who churned 
    user_window = Window.partitionBy("userId")
    df = df.withColumn('user_churned', max('churn_event').over(user_window))
    return df

def create_dataset(event_df): 
    '''
    event_df [pyspark dataframe] = raw dataframe
    Function to clean raw event-level data, transform it to user-level data, 
        create user level features, and label churned users. 
    data [pyspark dataframe] = final dataset for modeling
    '''
    print('Cleaning...')
    print('\tBefore: ', (event_df.count(), len(event_df.columns)))
    event_df = clean_data(event_df)
    print('\tAfter: ', (event_df.count(), len(event_df.columns)))
    
    print('Labeling...')
    event_df = label_churn(event_df)
    
    print('Generating features...')
    count_features = count_unique_features(event_df)
    print('\tcount_features: ', (count_features.count(), len(count_features.columns)))
    level_features = count_level_features(event_df)
    print('\tlevel_features: ', (level_features.count(), len(level_features.columns)))
    occurence_features = count_occurence_features(event_df)
    print('\toccurence_features: ', (occurence_features.count(), len(occurence_features.columns)))
    duration_features = get_duration_features(event_df)
    print('\tduration_features: ', (duration_features.count(), len(duration_features.columns)))
    categorical_features = dummy_cat_features(event_df)
    print('\tcategorical_features: ', (categorical_features.count(), len(categorical_features.columns)))
    
    # Transform event data to user data
    user_df = event_df.select('userId', 'user_churned').dropDuplicates()
    
    print('Putting it all together...')
    data = user_df.join(count_features,'userId','outer') \
                    .join(level_features,'userId','outer') \
                    .join(occurence_features,'userId','outer') \
                    .join(duration_features,'userId','outer') \
                    .join(categorical_features,'userId','outer') \
                    .dropDuplicates() \
                    .fillna(0)

    print('Done: ', (data.count(), len(data.columns)))
    return data


def main():
    # start spark session
    spark = create_spark_session()
    
    # read input data
    input_data = spark.read.json(INPUT_DATA)
    
    # create output data
    output_data = create_dataset(input_data)
    
    # write output data
    output_data.write.mode("overwrite").parquet(path=OUTPUT_DIRECTORY + 'model_dataset/')
    
    # stop spark session
    spark.stop()
    
    if__name__ == "__main__":
        main()