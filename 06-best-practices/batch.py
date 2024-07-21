#!/usr/bin/env python
# coding: utf-8

import os
import pickle
import pandas as pd
import fire

options = {
'client_kwargs': {
    'endpoint_url': os.getenv('S3_ENDPOINT_URL', 'http://localhost:4566')
    }
}

def prepare_data(df, categorical) -> pd.DataFrame:
    df['duration'] = df.tpep_dropoff_datetime - df.tpep_pickup_datetime
    df['duration'] = df.duration.dt.total_seconds() / 60

    df = df[(df.duration >= 1) & (df.duration <= 60)].copy()

    df[categorical] = df[categorical].fillna(-1).astype('int').astype('str')
    
    return df


def read_data(filename):
    if "s3" in filename:
        return pd.read_parquet(filename, storage_options=options)
    return pd.read_parquet(filename)


def save_result(df, y_pred, output_file):
    df_result = pd.DataFrame()
    df_result['ride_id'] = df['ride_id']
    df_result['predicted_duration'] = y_pred
    if "s3" in output_file:
        df_result.to_parquet(output_file, engine='pyarrow', index=False, storage_options=options)
    else:
        df_result.to_parquet(output_file, engine='pyarrow', index=False)


def get_input_path(year, month):
    default_input_pattern = 'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{year:04d}-{month:02d}.parquet'
    input_pattern = os.getenv('INPUT_FILE_PATTERN', default_input_pattern)
    return input_pattern.format(year=year, month=month)


def get_output_path(year, month):
    default_output_pattern = 's3://nyc-duration/out/{year:04d}-{month:02d}.parquet'
    output_pattern = os.getenv('OUTPUT_FILE_PATTERN', default_output_pattern)
    return output_pattern.format(year=year, month=month)


def main(year, month):
    year, month = int(year), int(month)
    input_file = get_input_path(year, month)
    output_file = get_output_path(year, month)
    with open('model.bin', 'rb') as f_in:
        dv, lr = pickle.load(f_in)


    categorical = ['PULocationID', 'DOLocationID']
    df = read_data(input_file)
    df = prepare_data(df, categorical)
    df['ride_id'] = f'{year:04d}/{month:02d}_' + df.index.astype('str')


    dicts = df[categorical].to_dict(orient='records')
    X_val = dv.transform(dicts)
    y_pred = lr.predict(X_val)


    print('predicted mean duration:', y_pred.mean())
    save_result(df, y_pred, output_file)



if __name__ == '__main__':
    fire.Fire(main)

