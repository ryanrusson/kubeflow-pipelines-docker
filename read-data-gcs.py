import argparse
from google.cloud import storage
import pandas as pd
import googleapiclient.discovery
import logging
import os

def get_data():
    # Get the data out of the GCS bucket
    DATA_BUCKET = 'gs://lending_club_toy_datasets/lending_club_data_v1.csv'
    return pd.read_csv(DATA_BUCKET)


def clean_data(df):
    # Drop columns with high cardinality
    df.drop('Id', axis=1, inplace=True)
    df.drop('Notes', axis=1, inplace=True)
    df.drop('purpose', axis=1, inplace=True)
    df.drop('emp_title', axis=1, inplace=True) 
    
    # Split out the datetime columns into years, months, days
    df.earliest_cr_line = pd.to_datetime(df.earliest_cr_line)
    df['cr_line_yrs'] = df.earliest_cr_line.dt.year
    df['cr_line_mths'] = df.earliest_cr_line.dt.month
    df['cr_line_days'] = df.earliest_cr_line.dt.day
    df.drop('earliest_cr_line', axis=1, inplace=True)
    
    # Now list out the final numerical and categorical columns
    num_cols = list(df._get_numeric_data().columns)
    cat_cols = list(set(df.columns) - set(df._get_numeric_data().columns))
    num_cols.remove('is_bad')
    target = ['is_bad']
    
    # Make sure all categorical columns that contain text have the same case
    for cat_col in cat_cols:
        df[cat_col] = df[cat_col].str.lower()
    
    # Also just make sure they are in fact strings
    df[cat_col] = df[cat_col].astype(str)  
        
    # Fill in missing values
    df['delinq_2yrs'] = df.delinq_2yrs.fillna(0)
    df['inq_last_6mths'].fillna(0, inplace=True)
    df['mths_since_last_delinq'].fillna(0, inplace=True)
    df['mths_since_last_record'].max()
    df['mths_since_last_record'].fillna(df.mths_since_last_record.max(), inplace=True)
    df['mths_since_last_record'] = df['mths_since_last_record'].astype(int)
    df['pub_rec'].fillna(0, inplace=True)
    df['pub_rec'] = df['pub_rec'].astype(int)
    df.drop('cr_line_days', axis=1, inplace=True)
    pd.options.mode.use_inf_as_na = True
    df['cr_line_mths'].fillna(df.cr_line_mths.mode()[0], inplace=True)  # 10.0 is the mode of the data
    df['cr_line_mths'] = df['cr_line_mths'].astype(int)
    df.drop('collections_12_mths_ex_med', axis=1, inplace=True)
    df['annual_inc'].fillna(df.annual_inc.median(), inplace=True)
    df['open_acc'].fillna(df.open_acc.median(), inplace=True)
    df['revol_util'].fillna(df.revol_util.median(), inplace=True)
    df['total_acc'].fillna(df.total_acc.median(), inplace=True)
    df['cr_line_yrs'].fillna(df.cr_line_yrs.median(), inplace=True)
    
    return df

def save_data(df):
    # upload the results to Cloud Storage
    #bucket_name ='gs://{{kfp-default-bucket}}'
    BUCKET_NAME = 'engaged-diode-275818-kubeflowpipelines-default'
    destination_blob_name = 'data-output.pkl'
    destination_file_name = 'data-output.pkl'
    df.to_pickle(destination_file_name)
    storage.Client().get_bucket(BUCKET_NAME).blob(destination_blob_name).upload_from_filename(destination_file_name)
    os.remove(destination_file_name)
    
    
def run():
    df = get_data()
    df = clean_data(df)
    save_data(df)


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.ERROR)
    run()
