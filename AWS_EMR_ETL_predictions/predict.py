import configparser
import os
from pyspark.sql import SparkSession
from pyspark.ml.feature import StandardScaler, VectorAssembler
from pyspark.ml.classification import RandomForestClassificationModel

config = configparser.ConfigParser()
config.read('dl.cfg', encoding='utf-8-sig')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']

INPUT_DIRECTORY = 's3a://sparkify/'
MODEL_DATA = "s3a://sparkify-model/churn_classifier.model"

features_list = ['page_count_Thumbs_Down',
 'page_count_Logout',
 'status_count_200',
 'method_count_PUT']

def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark

def vectorize_scale_features(data, features_list=data.columns[2:]):
    '''
    data [spark dataframe] = data of labels and features with numerical values
    features_list [list of strings] = list of names of feature columns
    Function to assemble features into a vector, scale it and return it with the labels
    input_data [spark dataframe] = dataframe of labels and scaled vectors
    '''
    # vectorize the features
    assembler = VectorAssembler(inputCols=features_list, outputCol='vector')
    data = assembler.transform(data)
    # scale the feature vectors
    scaler = StandardScaler(inputCol='vector', outputCol='features', withMean=True, withStd=True)
    scaler_fit = scaler.fit(data)
    data = scaler_fit.transform(data)
    # select labels and features
    input_data = data.select('label', 'features')
    return input_data

def main():
    # start spark session
    spark = create_spark_session()
    
    # read & prep data
    etl_data = os.path.join(INPUT_DIRECTORY + 'model_dataset/*/*/*/*.csv')
    input_data = spark.read.load(etl_data)
    train, test = input_data.randomSplit([0.7, 0.3], seed=42)
    processed_test = vectorize_scale_features(test, features_list)
    
    # load model
    fitted_model = RandomForestClassificationModel.load(MODEL_DATA)
    
    #create and save the predictions
    predicted_results = fitted_model.transform(processed_test)
    predicted_results.write.mode("overwrite").parquet(path=INPUT_DIRECTORY + 'churn_predictions/')
    
    # stop spark session
    spark.stop()
    
    if__name__ == "__main__":
        main()