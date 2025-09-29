#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
from __future__ import annotations

import os
import pickle
from typing import Dict, Tuple, Any

import mlflow
import numpy as np
import pandas as pd
import pendulum
from airflow.sdk import dag, task
from mlflow.models.signature import infer_signature
from scipy.sparse import spmatrix
from sklearn.feature_extraction import DictVectorizer
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_squared_error


@dag(
    schedule=None,
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    catchup=False,
    tags=["mlops", "nyc-taxi"],
    description='NYC Taxi Duration Prediction Pipeline',
)
def nyc_taxi_pipeline():
    """
    ### NYC Taxi Duration Prediction Pipeline
    
    This pipeline downloads NYC Yellow Taxi data, prepares it, trains a linear 
    regression model using pickup and dropoff locations as features, and registers
    the model with MLflow.
    
    The pipeline demonstrates how to use the Airflow TaskFlow API to create a 
    machine learning workflow.
    """
    
    @task()
    def download_data() -> str:
        """Download or load the NYC Yellow Taxi data for March 2023"""
        data_file = '/opt/airflow/data/yellow_tripdata_2023-03.parquet'
        
        # Create data directory if it doesn't exist
        os.makedirs('/opt/airflow/data', exist_ok=True)
        
        # If the file doesn't exist, download it
        if not os.path.exists(data_file):
            # URL for NYC Yellow Taxi data for March 2023
            url = 'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-03.parquet'
            df = pd.read_parquet(url)
            df.to_parquet(data_file)
            print(f"Data downloaded and saved to {data_file}")
        else:
            df = pd.read_parquet(data_file)
            print(f"Data loaded from {data_file}")
        
        # Count the number of rows in the DataFrame
        row_count = df.shape[0]
        print(f"Number of rows in the DataFrame: {row_count}")
        
        return data_file
    
    @task()
    def prepare_data(data_file: str) -> str:
        """Prepare the taxi data by calculating duration and filtering"""
        # Read the data
        df = pd.read_parquet(data_file)
        
        # Calculate the duration
        df['duration'] = df.tpep_dropoff_datetime - df.tpep_pickup_datetime
        df.duration = df.duration.dt.total_seconds() / 60
        
        # Filter for durations between 1 and 60 minutes
        df = df[(df.duration >= 1) & (df.duration <= 60)]
        
        # Convert categorical features
        categorical = ['PULocationID', 'DOLocationID']
        df[categorical] = df[categorical].astype(str)
        
        # Save the prepared data
        prepared_data_file = '/opt/airflow/data/prepared_taxi_data.parquet'
        df.to_parquet(prepared_data_file)
        
        # Count the number of rows in the prepared DataFrame
        row_count = df.shape[0]
        print(f"Number of rows in the prepared DataFrame: {row_count}")
        
        return prepared_data_file
    
    @task()
    def train_model(prepared_data_file: str) -> Dict[str, Any]:
        """Create features and train a linear regression model"""
        # Read the prepared data
        df = pd.read_parquet(prepared_data_file)
        
        # Keep only the required columns (PULocationID and DOLocationID)
        df_features = df[['PULocationID', 'DOLocationID']]
        
        # Convert dataframe to list of dictionaries with string conversion
        dicts = df_features.astype(str).to_dict(orient='records')
        
        # Create and fit a DictVectorizer
        dv = DictVectorizer()
        X = dv.fit_transform(dicts)
        
        # Get the dimensionality (number of columns)
        dimensionality = X.shape[1]
        print(f"Feature matrix shape: {X.shape}")
        print(f"Dimensionality (number of columns): {dimensionality}")
        
        # Target variable
        y = df['duration']
        
        # Train a linear regression model
        lr = LinearRegression()
        lr.fit(X, y)
        
        # Print the intercept (needed for the question)
        intercept = lr.intercept_
        print(f"Model intercept: {intercept:.2f}")
        
        # Make predictions on the training data
        y_pred = lr.predict(X)
        
        # Calculate RMSE on the training data
        rmse = np.sqrt(mean_squared_error(y, y_pred))
        print(f"RMSE on training data: {rmse:.4f}")
        
        # Save the model and vectorizer
        model_path = '/opt/airflow/models'
        os.makedirs(model_path, exist_ok=True)
        
        model_file = f'{model_path}/linear_regression_model.pkl'
        vectorizer_file = f'{model_path}/dict_vectorizer.pkl'
        
        with open(model_file, 'wb') as f:
            pickle.dump(lr, f)
        
        with open(vectorizer_file, 'wb') as f:
            pickle.dump(dv, f)
            
        # Return model files and metadata
        return {
            'model_file': model_file,
            'vectorizer_file': vectorizer_file,
            'prepared_data_file': prepared_data_file,
            'intercept': float(intercept),
            'rmse': float(rmse)
        }
    
    @task()
    def register_model(model_info: Dict[str, Any]) -> None:
        """Register the model with MLflow"""
        model_file = model_info['model_file']
        vectorizer_file = model_info['vectorizer_file']
        prepared_data_file = model_info['prepared_data_file']
        intercept = model_info['intercept']
        rmse = model_info['rmse']
        
        # Load the model and vectorizer
        with open(model_file, 'rb') as f:
            model = pickle.load(f)
        
        with open(vectorizer_file, 'rb') as f:
            dv = pickle.load(f)
        
        # Set MLflow tracking URI - using file system for local storage
        mlflow.set_tracking_uri('/opt/airflow/mlruns')
        
        # Start a new MLflow run
        with mlflow.start_run() as run:
            # Log model parameters
            mlflow.log_param("model_type", "LinearRegression")
            mlflow.log_param("intercept", intercept)
            
            # Log model metrics
            mlflow.log_metric("rmse", rmse)
            
            # Load X and y for the signature
            df = pd.read_parquet(prepared_data_file)
            df_features = df[['PULocationID', 'DOLocationID']]
            dicts = df_features.astype(str).to_dict(orient='records')
            X = dv.transform(dicts)
            y = df['duration'].values
            y_pred = model.predict(X)
            
            # Create model signature
            signature = infer_signature(X, y_pred)
            
            # Log the model with additional components
            mlflow_pyfunc_model_path = "model"
            
            # Log the scikit-learn model as an artifact
            mlflow.sklearn.log_model(
                sk_model=model,
                artifact_path=mlflow_pyfunc_model_path,
                signature=signature,
                input_example=X[:5],
                registered_model_name="yellow_taxi_duration_predictor"
            )
            
            # Log the DictVectorizer as a separate artifact
            dict_vectorizer_path = "dict_vectorizer"
            mlflow.sklearn.log_model(
                sk_model=dv,
                artifact_path=dict_vectorizer_path
            )
            
            # Print information about the registered model
            print(f"MLflow Run ID: {run.info.run_id}")
            print(f"Model saved in run {run.info.run_id}")
            
            # Get the path to the MLmodel file
            artifacts_uri = mlflow.get_artifact_uri()
            model_path = os.path.join(artifacts_uri, mlflow_pyfunc_model_path)
            print(f"Model artifacts saved at: {model_path}")
            print("Check the MLmodel file in this directory to find model_size_bytes")
    
    # Define the workflow by calling the tasks
    data_file = download_data()
    prepared_data_file = prepare_data(data_file)
    model_info = train_model(prepared_data_file)
    register_model(model_info)


# DAG invocation - this registers the DAG with Airflow
nyc_taxi_pipeline()