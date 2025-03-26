from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.task_group import TaskGroup
import pandas as pd
import os

# Define default arguments
default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'email': ['data_engineer@example.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

# Create DAG
with DAG(
    'data_processing_pipeline',
    default_args=default_args,
    description='Data processing pipeline with parallel tasks',
    schedule_interval='0 0 * * *',  # Run daily at midnight
    start_date=datetime(2025, 3, 20),
    catchup=False,
    tags=['data_pipeline', 'example'],
) as dag:
    
    # Start task
    start = EmptyOperator(
        task_id='start',
    )
    
    # Task to create directory for data
    create_dir = BashOperator(
        task_id='create_directory',
        bash_command='mkdir -p /tmp/airflow_data/',
    )
    
    # Download data task
    download_data = BashOperator(
        task_id='download_data',
        bash_command='curl -o /tmp/airflow_data/raw_data.csv https://example.com/data.csv',
    )
    
    # Create a task group for parallel processing
    with TaskGroup(group_id='process_data') as process_group:
        
        # Define processing functions
        def clean_data():
            """Clean the downloaded data by removing nulls and duplicates."""
            df = pd.read_csv('/tmp/airflow_data/raw_data.csv')
            # Remove duplicates
            df.drop_duplicates(inplace=True)
            # Remove rows with missing values
            df.dropna(inplace=True)
            # Save cleaned data
            df.to_csv('/tmp/airflow_data/cleaned_data.csv', index=False)
            return f"Cleaned data: {len(df)} rows remaining"
        
        def transform_data_1():
            """First transformation - extract user data."""
            df = pd.read_csv('/tmp/airflow_data/cleaned_data.csv')
            # Example transformation - extract user-related columns
            user_data = df[['user_id', 'username', 'email']]
            user_data.to_csv('/tmp/airflow_data/user_data.csv', index=False)
            return f"Transformed user data: {len(user_data)} entries"
        
        def transform_data_2():
            """Second transformation - extract transaction data."""
            df = pd.read_csv('/tmp/airflow_data/cleaned_data.csv')
            # Example transformation - extract transaction-related columns
            transaction_data = df[['transaction_id', 'user_id', 'amount', 'date']]
            transaction_data.to_csv('/tmp/airflow_data/transaction_data.csv', index=False)
            return f"Transformed transaction data: {len(transaction_data)} entries"
        
        # Create tasks for data processing
        clean_task = PythonOperator(
            task_id='clean_data',
            python_callable=clean_data,
        )
        
        transform_1 = PythonOperator(
            task_id='transform_data_1',
            python_callable=transform_data_1,
        )
        
        transform_2 = PythonOperator(
            task_id='transform_data_2',
            python_callable=transform_data_2,
        )
        
        # Set dependencies within the group
        clean_task >> [transform_1, transform_2]
    
    # Combine the processed data
    def combine_data():
        """Combine the transformed datasets into a final report."""
        users = pd.read_csv('/tmp/airflow_data/user_data.csv')
        transactions = pd.read_csv('/tmp/airflow_data/transaction_data.csv')
        
        # Merge datasets on user_id
        combined = pd.merge(transactions, users, on='user_id')
        
        # Create a summary report
        summary = combined.groupby('username').agg({
            'amount': ['sum', 'mean', 'count'],
            'transaction_id': 'count'
        })
        
        # Save the final report
        summary.to_csv('/tmp/airflow_data/final_report.csv')
        return f"Final report created with {len(summary)} user summaries"
    
    combine_task = PythonOperator(
        task_id='combine_data',
        python_callable=combine_data,
    )
    
    # Send notification
    notify = BashOperator(
        task_id='send_notification',
        bash_command='echo "Pipeline completed successfully. Report available at /tmp/airflow_data/final_report.csv" | mail -s "Data Pipeline Complete" data_analyst@example.com',
    )
    
    # Clean up temporary files
    cleanup = BashOperator(
        task_id='cleanup',
        bash_command='find /tmp/airflow_data -name "*.csv" -not -name "final_report.csv" -delete',
    )
    
    # End task
    end = EmptyOperator(
        task_id='end',
    )
    
    # Define task dependencies for the entire pipeline
    start >> create_dir >> download_data >> process_group >> combine_task >> notify >> cleanup >> end