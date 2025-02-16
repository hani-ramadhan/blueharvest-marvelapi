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

# Configure logging
logger = logging.getLogger(__name__)

class MarvelCharactersETL:
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
            
        logger.info(f"MarvelCharactersETL initialized for date: {self.current_date}")

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

    def get_last_processed_batch(self):
        """
        Get the last successfully processed batch number from logs
        Returns -1 if no previous batch exists
        """
        try:
            log_file = self.logs_dir / "fetch_progress.json"
            
            if not log_file.exists():
                logger.info("No previous fetch progress found, starting from beginning")
                return -1
            
            with open(log_file, 'r') as f:
                progress = json.load(f)
                last_batch = progress.get('last_processed_batch', -1)
                logger.info(f"Found last processed batch: {last_batch}")
                return last_batch
                
        except Exception as e:
            logger.error(f"Error reading last processed batch: {str(e)}")
            return -1

    def update_fetch_progress(self, batch_number, total_characters):
        """
        Update the fetch progress log with latest batch information
        """
        try:
            log_file = self.logs_dir / "fetch_progress.json"
            
            progress = {
                'date': self.current_date,
                'last_processed_batch': batch_number,
                'total_characters': total_characters,
                'last_updated': datetime.now().isoformat(),
                'status': 'in_progress'
            }
            
            with open(log_file, 'w') as f:
                json.dump(progress, f, indent=2)
                
            logger.info(
                f"Updated fetch progress for {self.current_date}: "
                f"Batch {batch_number}, Total characters: {total_characters}"
            )
            
        except Exception as e:
            logger.error(f"Failed to update fetch progress: {str(e)}")
            raise AirflowException(f"Progress update failed: {str(e)}")

    def validate_character_count(self, **context):
        """
        Layer 3: Validate character counts from raw data
        - Reads raw JSON files from the date directory
        - Verifies comic counts against API
        - Logs validation results
        """
        try:
            validation_results = []
            validation_issues = []
            process_date = context['task_instance'].xcom_pull(
                task_ids='fetch_characters',
                key='process_date'
            )
            
            # Get all batch files for the current date
            batch_files = sorted(self.raw_dir.glob("batch_*.json"))
            logger.info(f"Starting validation for {len(batch_files)} batch files")
            
            for batch_file in batch_files:
                with open(batch_file, 'r') as f:
                    characters = json.load(f)
                logger.info(f"Processing {batch_file}")
                for character in characters:
                    try:
                        # Basic validation
                        if not all(k in character for k in ['id', 'name', 'comics']):
                            validation_issues.append({
                                'character_id': character.get('id', 'unknown'),
                                'name': character.get('name', 'unknown'),
                                'issue': 'Missing required fields',
                                'timestamp': datetime.now().isoformat()
                            })
                            continue
                        
                        # Verify comics count
                        auth_params = self.generate_auth_params()
                        response = requests.get(
                            f"{self.base_url}/characters/{character['id']}/comics",
                            params={**auth_params, 'limit': 1},
                            timeout=30
                        )
                        response.raise_for_status()
                        
                        api_comic_count = response.json()['data']['total']
                        stored_comic_count = character['comics']['available']
                        
                        # Record validation result
                        validation_result = {
                            'id': character['id'],
                            'name': character['name'],
                            'stored_comic_count': stored_comic_count,
                            'api_comic_count': api_comic_count,
                            'description': character.get('description', ''),
                            'modified': character['modified'],
                            'validation_date': datetime.now().isoformat(),
                            'is_valid': api_comic_count == stored_comic_count
                        }
                        
                        validation_results.append(validation_result)
                        
                        if api_comic_count != stored_comic_count:
                            validation_issues.append({
                                'character_id': character['id'],
                                'name': character['name'],
                                'stored_count': stored_comic_count,
                                'api_count': api_comic_count,
                                'issue': 'Comic count mismatch',
                                'timestamp': datetime.now().isoformat()
                            })
                        
                        time.sleep(0.1)  # Rate limiting
                        
                    except Exception as e:
                        validation_issues.append({
                            'character_id': character.get('id', 'unknown'),
                            'name': character.get('name', 'unknown'),
                            'issue': str(e),
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

    def process_to_parquet(self, **context):
        """
        Layer 4: Process validated data to Parquet format
        - Reads validation results
        - Transforms data into final schema
        - Saves as Parquet file with date partitioning
        """
        try:
            process_date = context['task_instance'].xcom_pull(
                task_ids='fetch_characters',
                key='process_date'
            )
            
            # Read validation results
            validation_file = self.logs_dir / "validation_results.json"
            with open(validation_file, 'r') as f:
                validation_data = json.load(f)
            
            # Prepare data for Parquet
            characters_data = []
            for result in validation_data['results']:
                character = {
                    'character_id': result['id'],
                    'name': result['name'],
                    'comic_count': result['api_comic_count'],  # Use verified count
                    'description': result['description'],
                    'last_modified': result['modified'],
                    'process_date': process_date,
                    'is_valid': result['is_valid']
                }
                characters_data.append(character)
            
            # Create processed directory for date if it doesn't exist
            processed_dir = self.data_dir / "processed" / process_date
            processed_dir.mkdir(parents=True, exist_ok=True)
            
            # Save as Parquet
            import pandas as pd
            import pyarrow as pa
            import pyarrow.parquet as pq
            
            df = pd.DataFrame(characters_data)
            table = pa.Table.from_pandas(df)
            
            parquet_file = processed_dir / "characters.parquet"
            pq.write_table(
                table,
                parquet_file,
                compression='snappy'
            )
            
            # Log processing summary
            processing_summary = {
                'date': process_date,
                'total_processed': len(characters_data),
                'valid_records': sum(1 for c in characters_data if c['is_valid']),
                'parquet_file': str(parquet_file),
                'timestamp': datetime.now().isoformat()
            }
            
            with open(self.logs_dir / "processing_summary.json", 'w') as f:
                json.dump(processing_summary, f, indent=2)
            
            # Push metadata to XCom
            context['task_instance'].xcom_push(
                key='processed_count',
                value=len(characters_data)
            )
            
            logger.info(
                f"Processing completed. Total processed: {len(characters_data)}, "
                f"Saved to: {parquet_file}"
            )
            return len(characters_data)
            
        except Exception as e:
            logger.error(f"Processing failed: {str(e)}")
            raise AirflowException(str(e))

    def fetch_characters(self, **context):
        """
        Layer 1: Fetch characters from Marvel API with resumability
        """
        try:
            # Get last processed batch
            last_batch = self.get_last_processed_batch()
            offset = (last_batch + 1) * 100  # 100 is the batch size
            limit = 100
            batch_number = last_batch + 1
            total_characters = 0
            
            logger.info(
                f"Starting character fetch for {self.current_date} "
                f"from batch {batch_number} (offset: {offset})"
            )
            
            while True:
                try:
                    # Get authentication parameters
                    auth_params = self.generate_auth_params()
                    
                    # Make API request
                    logger.info(f"Fetching batch {batch_number} (offset: {offset})")
                    response = requests.get(
                        f"{self.base_url}/characters",
                        params={
                            **auth_params,
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
                        logger.info("No more characters to fetch")
                        break
                    
                    # Store raw batch with date in filename
                    batch_file = self.raw_dir / f"batch_{str(batch_number).zfill(4)}.json"
                    with open(batch_file, 'w') as f:
                        json.dump(results, f, indent=2)
                    
                    # Update totals and progress
                    total_characters += len(results)
                    total_available = data['data']['total']
                    
                    self.update_fetch_progress(batch_number, total_characters)
                    logger.info(
                        f"Stored batch {batch_number} for {self.current_date} "
                        f"({total_characters}/{total_available} characters)"
                    )
                    
                    # Check if we've fetched everything
                    if total_characters >= total_available:
                        logger.info("Fetched all available characters")
                        break
                    
                    # Prepare for next batch
                    offset += limit
                    batch_number += 1
                    time.sleep(1)  # Rate limiting
                    
                except requests.exceptions.RequestException as e:
                    logger.error(f"Failed to fetch batch {batch_number}: {str(e)}")
                    raise AirflowException(f"API request failed: {str(e)}")
            
            # Update final progress
            final_progress = {
                'date': self.current_date,
                'last_processed_batch': batch_number,
                'total_characters': total_characters,
                'last_updated': datetime.now().isoformat(),
                'status': 'completed'
            }
            
            with open(self.logs_dir / "fetch_progress.json", 'w') as f:
                json.dump(final_progress, f, indent=2)
            
            # Store execution summary
            summary = {
                'date': self.current_date,
                'total_batches': batch_number + 1,
                'total_characters': total_characters,
                'execution_time': datetime.now().isoformat(),
                'status': 'success'
            }
            
            with open(self.logs_dir / "execution_summary.json", 'w') as f:
                json.dump(summary, f, indent=2)
            
            # Push metadata to XCom for next task
            context['task_instance'].xcom_push(
                key='character_count',
                value=total_characters
            )
            context['task_instance'].xcom_push(
                key='process_date',
                value=self.current_date
            )
            
            logger.info(
                f"Character fetch completed for {self.current_date}. "
                f"Total characters: {total_characters}"
            )
            return total_characters
            
        except Exception as e:
            logger.error(f"Character fetch failed: {str(e)}")
            raise AirflowException(str(e))

    
    def create_dashboard_csv(self, **context):
        """
        Layer 5: Create analytics-ready CSV for Streamlit dashboard
        - Reads latest Parquet file
        - Creates dashboard-ready format
        - Saves latest state as CSV
        - Maintains refresh logs
        """
        try:
            process_date = context['task_instance'].xcom_pull(
                task_ids='fetch_characters',
                key='process_date'
            )
            
            # Read the Parquet file for the current date
            processed_dir = self.data_dir / "processed" / process_date
            parquet_file = processed_dir / "characters.parquet"
            
            if not parquet_file.exists():
                raise FileNotFoundError(f"Parquet file not found: {parquet_file}")
            
            # Create analytics directory for date
            analytics_dir = self.data_dir / "analytics" / process_date
            analytics_dir.mkdir(parents=True, exist_ok=True)
            
            # Read Parquet and prepare dashboard data
            import pandas as pd
            df = pd.read_parquet(parquet_file)
            
            # Create dashboard view
            dashboard_df = df[[
                'character_id', 
                'name', 
                'comic_count', 
                'description'
            ]].copy()
            
            # Add any dashboard-specific calculations
            dashboard_df['last_updated'] = process_date
            
            # Save latest state for dashboard
            latest_csv = self.data_dir / "analytics" / "latest" / "characters.csv"
            latest_csv.parent.mkdir(parents=True, exist_ok=True)
            dashboard_df.to_csv(latest_csv, index=False)
            
            # Also save date-specific version
            dated_csv = analytics_dir / "characters.csv"
            dashboard_df.to_csv(dated_csv, index=False)
            
            # Create refresh log
            refresh_info = {
                'process_date': process_date,
                'record_count': len(dashboard_df),
                'latest_csv_path': str(latest_csv),
                'dated_csv_path': str(dated_csv),
                'refresh_timestamp': datetime.now().isoformat(),
                'columns': list(dashboard_df.columns)
            }
            
            # Save refresh log
            refresh_log = self.logs_dir / "dashboard_refresh.json"
            with open(refresh_log, 'w') as f:
                json.dump(refresh_info, f, indent=2)
            
            # Create a simplified last_refresh file for dashboard
            last_refresh = {
                'last_update': process_date,
                'record_count': len(dashboard_df),
                'refresh_timestamp': datetime.now().isoformat()
            }
            
            last_refresh_file = self.data_dir / "analytics" / "latest" / "last_refresh.json"
            with open(last_refresh_file, 'w') as f:
                json.dump(last_refresh, f, indent=2)
            
            # Push metadata to XCom
            context['task_instance'].xcom_push(
                key='dashboard_record_count',
                value=len(dashboard_df)
            )
            
            logger.info(
                f"Dashboard CSV created. Total records: {len(dashboard_df)}, "
                f"Latest file: {latest_csv}"
            )
            return len(dashboard_df)
            
        except Exception as e:
            logger.error(f"Dashboard CSV creation failed: {str(e)}")
            raise AirflowException(str(e))
        
# Define DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 5,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    'marvel_characters_initial_load',
    default_args=default_args,
    description='Initial load of Marvel characters data',
    schedule_interval=None,  # Manual trigger only
    start_date=datetime(2024, 2, 16),
    catchup=False,
    tags=['marvel', 'initial_load'],
) as dag:
    
    etl = MarvelCharactersETL()

    # DAG tasks
    fetch_task = PythonOperator(
        task_id='fetch_characters',
        python_callable=etl.fetch_characters,
        provide_context=True,
    )
    
    validate_task = PythonOperator(
        task_id='validate_character_count',
        python_callable=etl.validate_character_count,
        provide_context=True,
    )
    
    process_task = PythonOperator(
        task_id='process_to_parquet',
        python_callable=etl.process_to_parquet,
        provide_context=True,
    )
    
    dashboard_task = PythonOperator(
        task_id='create_dashboard_csv',
        python_callable=etl.create_dashboard_csv,
        provide_context=True,
    )
    
    # Set task dependencies
    fetch_task >> validate_task >> process_task >> dashboard_task