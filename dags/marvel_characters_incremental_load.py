from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowException
from datetime import datetime, timedelta
import hashlib
import time
import json
import requests
from pathlib import Path
import logging
import os
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

# Configure logging
logger = logging.getLogger(__name__)

class MarvelCharactersIncrementalETL:
    def __init__(self):
        """Initialize the ETL process with necessary configurations"""
        # Fetch API keys from environment file
        self.public_key = os.getenv('MARVEL_PUBLIC_KEY')
        self.private_key = os.getenv('MARVEL_PRIVATE_KEY')
        self.base_url = "https://gateway.marvel.com/v1/public"
        
        # Get current date for directory structure
        self.current_date = datetime.now().strftime('%Y%m%d')
        
        # Setup directory structure with date
        self.data_dir = Path("/opt/airflow/data/marvel")
        self.raw_dir = self.data_dir / "raw" / self.current_date
        self.logs_dir = self.data_dir / "logs" / self.current_date
        
        # Create directories if they don't exist
        for dir_path in [self.raw_dir, self.logs_dir]:
            dir_path.mkdir(parents=True, exist_ok=True)
            
        logger.info(f"MarvelCharactersIncrementalETL initialized for date: {self.current_date}")

    def generate_auth_params(self):
        """Generate authentication parameters for Marvel API"""
        try:
            if not self.public_key or not self.private_key:
                raise ValueError("Marvel API keys not found in environment variables")
                
            timestamp = str(int(time.time()))
            hash_input = timestamp + self.private_key + self.public_key
            hash_value = hashlib.md5(hash_input.encode('utf-8')).hexdigest()
            
            return {
                'ts': timestamp,
                'apikey': self.public_key,
                'hash': hash_value
            }
        except Exception as e:
            logger.error(f"Failed to generate auth parameters: {str(e)}")
            raise AirflowException(f"Authentication generation failed: {str(e)}")

    def get_last_sync_time(self):
        """Get the timestamp of the last successful sync"""
        try:
            last_refresh_file = self.data_dir / "analytics" / "latest" / "last_refresh.json"
            if not last_refresh_file.exists():
                logger.warning("No previous sync found, defaulting to 24 hours ago")
                return (datetime.now() - timedelta(days=1)).isoformat()
            
            with open(last_refresh_file, 'r') as f:
                last_refresh = json.load(f)
                return last_refresh['last_update']
                
        except Exception as e:
            logger.error(f"Error reading last sync time: {str(e)}")
            return (datetime.now() - timedelta(days=1)).isoformat()

    def fetch_modified_characters(self, **context):
        """
        Layer 1: Fetch only characters modified since last sync
        """
        try:
            last_sync = self.get_last_sync_time()
            modified_characters = []
            offset = 0
            limit = 100
            total_modified = 0
            
            logger.info(f"Starting incremental fetch for characters modified since {last_sync}")
            
            while True:
                try:
                    # Get authentication parameters
                    auth_params = self.generate_auth_params()
                    
                    # Make API request with modifiedSince parameter
                    response = requests.get(
                        f"{self.base_url}/characters",
                        params={
                            **auth_params,
                            'modifiedSince': last_sync,
                            'offset': offset,
                            'limit': limit
                        },
                        timeout=30
                    )
                    response.raise_for_status()
                    
                    # Parse response
                    data = response.json()
                    results = data['data']['results']
                    
                    if not results:
                        break
                    
                    modified_characters.extend(results)
                    total_modified += len(results)
                    
                    # Store batch
                    batch_file = self.raw_dir / f"modified_batch_{offset}.json"
                    with open(batch_file, 'w') as f:
                        json.dump(results, f, indent=2)
                    
                    logger.info(f"Stored {len(results)} modified characters (offset: {offset})")
                    
                    if total_modified >= data['data']['total']:
                        break
                    
                    offset += limit
                    time.sleep(0.5)  # Rate limiting
                    
                except requests.exceptions.RequestException as e:
                    logger.error(f"Failed to fetch modified characters: {str(e)}")
                    raise AirflowException(f"API request failed: {str(e)}")
            
            # Store execution summary
            summary = {
                'date': self.current_date,
                'last_sync': last_sync,
                'total_modified': total_modified,
                'execution_time': datetime.now().isoformat(),
                'status': 'success'
            }
            
            with open(self.logs_dir / "incremental_summary.json", 'w') as f:
                json.dump(summary, f, indent=2)
            
            # Push metadata to XCom
            context['task_instance'].xcom_push(
                key='modified_count',
                value=total_modified
            )
            context['task_instance'].xcom_push(
                key='process_date',
                value=self.current_date
            )
            
            logger.info(f"Incremental fetch completed. Modified characters: {total_modified}")
            return total_modified
            
        except Exception as e:
            logger.error(f"Incremental fetch failed: {str(e)}")
            raise AirflowException(str(e))

    
    def validate_modified_characters(self, **context):
        """
        Simplified validation that doesn't require additional API calls:
        1. Schema validation
        2. Data type checks
        3. Value range validations
        4. Basic data quality checks
        """
        try:
            validation_results = []
            validation_issues = []
            process_date = context['task_instance'].xcom_pull(
                task_ids='fetch_modified_characters', 
                key='process_date'
            )
            
            # Get all batch files
            batch_files = sorted(self.raw_dir.glob("modified_batch_*.json")) 
            logger.info(f"Starting validation for {len(batch_files)} batch files")
            
            required_fields = {'id', 'name', 'comics', 'modified'}
            
            for batch_file in batch_files:
                with open(batch_file, 'r') as f:
                    characters = json.load(f)
                
                logger.info(f"Validating {len(characters)} characters from {batch_file}")
                
                for character in characters:
                    validation_result = {
                        'id': character.get('id', 'unknown'),
                        'name': character.get('name', 'unknown'),
                        'stored_comic_count': character.get('comics', {}).get('available', 0),
                        'modified': character.get('modified', ''),
                        'validation_date': datetime.now().isoformat(),
                        'is_valid': True,
                        'issues': []
                    }
                    
                    # 1. Schema Validation
                    missing_fields = required_fields - set(character.keys())
                    if missing_fields:
                        validation_result['is_valid'] = False
                        validation_result['issues'].append(f"Missing required fields: {missing_fields}")

                    # 2. Data Type Validation
                    if not isinstance(character.get('id'), int):
                        validation_result['is_valid'] = False
                        validation_result['issues'].append("Invalid ID type")
                        
                    if not isinstance(character.get('name'), str):
                        validation_result['is_valid'] = False
                        validation_result['issues'].append("Invalid name type")
                    
                    comics = character.get('comics', {})
                    if not isinstance(comics.get('available'), int):
                        validation_result['is_valid'] = False
                        validation_result['issues'].append("Invalid comics count type")

                    # 3. Value Range Validation
                    if character.get('comics', {}).get('available', 0) < 0:
                        validation_result['is_valid'] = False
                        validation_result['issues'].append("Negative comics count")

                    # 4. Data Quality Checks
                    if not character.get('name', '').strip():
                        validation_result['is_valid'] = False
                        validation_result['issues'].append("Empty name")

                    validation_results.append(validation_result)
                    
                    if not validation_result['is_valid']:
                        validation_issues.append({
                            'character_id': validation_result['id'],
                            'name': validation_result['name'],
                            'issues': validation_result['issues'],
                            'timestamp': datetime.now().isoformat()
                        })

            # Save validation results
            validation_file = self.logs_dir / "validation_results.json"
            issues_file = self.logs_dir / "validation_issues.json"
            
            with open(validation_file, 'w') as f:
                json.dump({
                    'date': process_date,
                    'results': validation_results,
                    'total_validated': len(validation_results),
                    'total_issues': len(validation_issues),
                    'timestamp': datetime.now().isoformat()
                }, f, indent=2)
            
            with open(issues_file, 'w') as f:
                json.dump({
                    'date': process_date,
                    'issues': validation_issues,
                    'total_issues': len(validation_issues),
                    'timestamp': datetime.now().isoformat()
                }, f, indent=2)
            
            # Push metadata to XCom
            context['task_instance'].xcom_push(
                key='validated_count',
                value=len(validation_results)
            )
            context['task_instance'].xcom_push(
                key='issues_count',
                value=len(validation_issues)
            )
            
            logger.info(
                f"Validation completed. Total validated: {len(validation_results)}, "
                f"Issues found: {len(validation_issues)}"
            )
            return len(validation_results)
            
        except Exception as e:
            logger.error(f"Validation failed: {str(e)}")
            raise AirflowException(str(e))
    # def validate_modified_characters(self, **context):
    #     """
    #     Layer 3: Validate modified character data
    #     """
    #     try:
    #         validation_results = []
    #         validation_issues = []
            
    #         # Get all batch files for modified characters
    #         batch_files = sorted(self.raw_dir.glob("modified_batch_*.json"))
    #         logger.info(f"Starting validation for {len(batch_files)} modified character batches")
            
    #         for batch_file in batch_files:
    #             with open(batch_file, 'r') as f:
    #                 characters = json.load(f)
                
    #             for character in characters:
    #                 try:
    #                     # Basic validation
    #                     if not all(k in character for k in ['id', 'name', 'comics']):
    #                         validation_issues.append({
    #                             'character_id': character.get('id', 'unknown'),
    #                             'name': character.get('name', 'unknown'),
    #                             'issue': 'Missing required fields',
    #                             'timestamp': datetime.now().isoformat()
    #                         })
    #                         continue
                        
    #                     # Verify comics count
    #                     auth_params = self.generate_auth_params()
    #                     response = requests.get(
    #                         f"{self.base_url}/characters/{character['id']}/comics",
    #                         params={**auth_params, 'limit': 1},
    #                         timeout=30
    #                     )
    #                     response.raise_for_status()
                        
    #                     api_comic_count = response.json()['data']['total']
    #                     stored_comic_count = character['comics']['available']
                        
    #                     validation_result = {
    #                         'id': character['id'],
    #                         'name': character['name'],
    #                         'stored_comic_count': stored_comic_count,
    #                         'api_comic_count': api_comic_count,
    #                         'description': character.get('description', ''),
    #                         'modified': character['modified'],
    #                         'validation_date': datetime.now().isoformat(),
    #                         'is_valid': api_comic_count == stored_comic_count
    #                     }
                        
    #                     validation_results.append(validation_result)
                        
    #                     if api_comic_count != stored_comic_count:
    #                         validation_issues.append({
    #                             'character_id': character['id'],
    #                             'name': character['name'],
    #                             'stored_count': stored_comic_count,
    #                             'api_count': api_comic_count,
    #                             'issue': 'Comic count mismatch',
    #                             'timestamp': datetime.now().isoformat()
    #                         })
                        
    #                     time.sleep(0.1)
                        
    #                 except Exception as e:
    #                     validation_issues.append({
    #                         'character_id': character.get('id', 'unknown'),
    #                         'name': character.get('name', 'unknown'),
    #                         'issue': str(e),
    #                         'timestamp': datetime.now().isoformat()
    #                     })
            
    #         # Save validation results
    #         with open(self.logs_dir / "incremental_validation.json", 'w') as f:
    #             json.dump({
    #                 'date': self.current_date,
    #                 'results': validation_results,
    #                 'total_validated': len(validation_results),
    #                 'total_issues': len(validation_issues),
    #                 'timestamp': datetime.now().isoformat()
    #             }, f, indent=2)
            
    #         logger.info(
    #             f"Validation completed. Total validated: {len(validation_results)}, "
    #             f"Issues found: {len(validation_issues)}"
    #         )
    #         return len(validation_results)
            
    #     except Exception as e:
    #         logger.error(f"Validation failed: {str(e)}")
    #         raise AirflowException(str(e))

    def update_parquet_data(self, **context):
        """
        Layer 4: Update existing Parquet data with modified characters
        """
        try:
            # Read validation results
            with open(self.logs_dir / "incremental_validation.json", 'r') as f:
                validation_data = json.load(f)
            
            # Prepare modified character data
            modified_characters = []
            for result in validation_data['results']:
                character = {
                    'character_id': result['id'],
                    'name': result['name'],
                    'comic_count': result['stored_comic_count'],
                    'last_modified': result['modified'],
                    'process_date': self.current_date,
                    'is_valid': result['is_valid']
                }
                modified_characters.append(character)
            
            # Create DataFrame for modified characters
            modified_df = pd.DataFrame(modified_characters)

            
            # Read existing Parquet file
            processed_dir = self.data_dir / "processed"
            existing_parquet = processed_dir / self.current_date / "characters.parquet"
            
            if existing_parquet.exists():
                existing_df = pd.read_parquet(existing_parquet)

                print(existing_df.head())
                print(modified_df.head())

                if len(modified_df) == 0:
                    updated_df = existing_df
                else:
                
                    # Remove modified characters from existing data
                    existing_df = existing_df[~existing_df['character_id'].isin(modified_df['character_id'])]
                    
                    # Append modified characters
                    updated_df = pd.concat([existing_df, modified_df], ignore_index=True)
            else:
                updated_df = modified_df
            
            # Save updated Parquet file
            parquet_dir = processed_dir / self.current_date
            parquet_dir.mkdir(parents=True, exist_ok=True)
            
            table = pa.Table.from_pandas(updated_df)
            pq.write_table(
                table,
                parquet_dir / "characters.parquet",
                compression='snappy'
            )
            
            # Log processing summary
            processing_summary = {
                'date': self.current_date,
                'total_modified': len(modified_characters),
                'total_records': len(updated_df),
                'parquet_file': str(parquet_dir / "characters.parquet"),
                'timestamp': datetime.now().isoformat()
            }
            
            with open(self.logs_dir / "incremental_processing.json", 'w') as f:
                json.dump(processing_summary, f, indent=2)
            
            logger.info(
                f"Parquet update completed. Modified records: {len(modified_characters)}, "
                f"Total records: {len(updated_df)}"
            )
            return len(modified_characters)
            
        except Exception as e:
            logger.error(f"Parquet update failed: {str(e)}")
            raise AirflowException(str(e))

    def update_dashboard_data(self, **context):
        """
        Layer 5: Update dashboard CSV with latest data
        """
        try:
            # Read updated Parquet file
            processed_dir = self.data_dir / "processed" / self.current_date
            parquet_file = processed_dir / "characters.parquet"
            
            if not parquet_file.exists():
                raise FileNotFoundError(f"Parquet file not found: {parquet_file}")
            
            # Read Parquet and prepare dashboard data
            df = pd.read_parquet(parquet_file)
            
            # Create dashboard view
            dashboard_df = df[[
                'character_id',
                'name',
                'comic_count'
            ]].copy()
            
            dashboard_df['last_updated'] = self.current_date
            
            # Save latest state
            latest_dir = self.data_dir / "analytics" / "latest"
            latest_dir.mkdir(parents=True, exist_ok=True)
            
            latest_csv = latest_dir / "characters.csv"
            dashboard_df.to_csv(latest_csv, index=False)
            
            # Save date-specific version
            analytics_dir = self.data_dir / "analytics" / self.current_date
            analytics_dir.mkdir(parents=True, exist_ok=True)
            
            dated_csv = analytics_dir / "characters.csv"
            dashboard_df.to_csv(dated_csv, index=False)
            
            # Update refresh logs
            refresh_info = {
                'last_update': self.current_date,
                'record_count': len(dashboard_df),
                'refresh_timestamp': datetime.now().isoformat()
            }
            
            with open(latest_dir / "last_refresh.json", 'w') as f:
                json.dump(refresh_info, f, indent=2)
            
            logger.info(
                f"Dashboard data updated. Total records: {len(dashboard_df)}"
            )
            return len(dashboard_df)
            
        except Exception as e:
            logger.error(f"Dashboard update failed: {str(e)}")
            raise AirflowException(str(e))

# Define DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=1),
}

# Key Changes from Initial Load DAG:
# 1. Changed schedule_interval to daily instead of None (manual)
# 2. Modified task names to reflect incremental nature
# 3. Adjusted retry settings for more frequent but shorter retries
# 4. Added tags to distinguish from initial load
with DAG(
    'marvel_characters_incremental_load',
    default_args=default_args,
    description='Daily incremental load of modified Marvel characters',
    schedule_interval='0 0 * * *',  # Run daily at midnight
    start_date=datetime(2024, 2, 16),
    catchup=False,
    tags=['marvel', 'incremental_load', 'daily'],
) as dag:
    
    etl = MarvelCharactersIncrementalETL()

    # DAG tasks with modified names to reflect incremental nature
    fetch_modified_task = PythonOperator(
        task_id='fetch_modified_characters',
        python_callable=etl.fetch_modified_characters,
        provide_context=True,
    )
    
    validate_modified_task = PythonOperator(
        task_id='validate_modified_characters',
        python_callable=etl.validate_modified_characters,
        provide_context=True,
    )
    
    update_parquet_task = PythonOperator(
        task_id='update_parquet_data',
        python_callable=etl.update_parquet_data,
        provide_context=True,
    )
    
    update_dashboard_task = PythonOperator(
        task_id='update_dashboard_data',
        python_callable=etl.update_dashboard_data,
        provide_context=True,
    )
    
    # Set task dependencies (same flow as initial load)
    fetch_modified_task >> validate_modified_task >> update_parquet_task >> update_dashboard_task