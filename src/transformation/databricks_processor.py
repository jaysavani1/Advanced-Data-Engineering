"""
Databricks Data Processor for Olympic Analytics Platform.
Handles data transformation and processing using Azure Databricks.
"""

import json
import time
from datetime import datetime
from typing import Dict, Any, List, Optional
from dataclasses import dataclass

from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient
from azure.storage.filedatalake import DataLakeServiceClient

from ..utils.config import get_config
from ..utils.logger import PipelineLogger


@dataclass
class ProcessingJob:
    """Represents a data processing job."""
    job_id: str
    notebook_path: str
    parameters: Dict[str, Any]
    status: str
    start_time: datetime
    end_time: Optional[datetime] = None
    error_message: Optional[str] = None


class DatabricksProcessor:
    """Handles data processing using Azure Databricks."""
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize Databricks processor.
        
        Args:
            config: Databricks configuration
        """
        self.config = config
        self.databricks_config = config.get('databricks', {})
        self.storage_config = config.get('storage', {})
        self.logger = PipelineLogger("databricks_processor", config.get('logging', {}))
        self.credential = DefaultAzureCredential()
        self.jobs = {}
        
        self._setup_storage_client()
    
    def _setup_storage_client(self) -> None:
        """Set up Azure Data Lake Storage client."""
        try:
            account_name = self.storage_config.get('account_name')
            account_key = self.storage_config.get('account_key')
            
            self.storage_client = DataLakeServiceClient(
                account_url=f"https://{account_name}.dfs.core.windows.net",
                credential=account_key
            )
            
            self.logger.logger.info(
                "Storage client initialized",
                account_name=account_name
            )
            
        except Exception as e:
            self.logger.pipeline_error(e, setup_phase=True)
            raise
    
    def process_athletes_data(self, input_path: str, output_path: str) -> ProcessingJob:
        """Process athletes data using Databricks notebook."""
        job = ProcessingJob(
            job_id=f"athletes_processing_{int(time.time())}",
            notebook_path=self.databricks_config.get('notebooks', {}).get('data_processing'),
            parameters={
                'input_path': input_path,
                'output_path': output_path,
                'data_type': 'athletes'
            },
            status='pending',
            start_time=datetime.utcnow()
        )
        
        self.jobs[job.job_id] = job
        self.logger.start_pipeline(job_id=job.job_id, data_type='athletes')
        
        try:
            # Here you would submit the job to Databricks
            # For now, we'll simulate the processing
            self._simulate_processing(job)
            
        except Exception as e:
            job.status = 'failed'
            job.error_message = str(e)
            job.end_time = datetime.utcnow()
            self.logger.pipeline_error(e, job_id=job.job_id)
        
        return job
    
    def process_medals_data(self, input_path: str, output_path: str) -> ProcessingJob:
        """Process medals data using Databricks notebook."""
        job = ProcessingJob(
            job_id=f"medals_processing_{int(time.time())}",
            notebook_path=self.databricks_config.get('notebooks', {}).get('data_processing'),
            parameters={
                'input_path': input_path,
                'output_path': output_path,
                'data_type': 'medals'
            },
            status='pending',
            start_time=datetime.utcnow()
        )
        
        self.jobs[job.job_id] = job
        self.logger.start_pipeline(job_id=job.job_id, data_type='medals')
        
        try:
            self._simulate_processing(job)
            
        except Exception as e:
            job.status = 'failed'
            job.error_message = str(e)
            job.end_time = datetime.utcnow()
            self.logger.pipeline_error(e, job_id=job.job_id)
        
        return job
    
    def process_teams_data(self, input_path: str, output_path: str) -> ProcessingJob:
        """Process teams data using Databricks notebook."""
        job = ProcessingJob(
            job_id=f"teams_processing_{int(time.time())}",
            notebook_path=self.databricks_config.get('notebooks', {}).get('data_processing'),
            parameters={
                'input_path': input_path,
                'output_path': output_path,
                'data_type': 'teams'
            },
            status='pending',
            start_time=datetime.utcnow()
        )
        
        self.jobs[job.job_id] = job
        self.logger.start_pipeline(job_id=job.job_id, data_type='teams')
        
        try:
            self._simulate_processing(job)
            
        except Exception as e:
            job.status = 'failed'
            job.error_message = str(e)
            job.end_time = datetime.utcnow()
            self.logger.pipeline_error(e, job_id=job.job_id)
        
        return job
    
    def _simulate_processing(self, job: ProcessingJob) -> None:
        """Simulate data processing (replace with actual Databricks job submission)."""
        # Simulate processing time
        time.sleep(2)
        
        job.status = 'completed'
        job.end_time = datetime.utcnow()
        
        duration = (job.end_time - job.start_time).total_seconds()
        self.logger.end_pipeline(duration, job_id=job.job_id)
    
    def get_job_status(self, job_id: str) -> Optional[ProcessingJob]:
        """Get the status of a processing job."""
        return self.jobs.get(job_id)
    
    def get_all_jobs(self) -> List[ProcessingJob]:
        """Get all processing jobs."""
        return list(self.jobs.values())


class DataTransformer:
    """Main data transformation orchestrator."""
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize data transformer.
        
        Args:
            config: Configuration dictionary
        """
        self.config = config
        self.processor = DatabricksProcessor(config)
        self.logger = PipelineLogger("data_transformer", config.get('logging', {}))
    
    def transform_all_data(self) -> List[ProcessingJob]:
        """Transform all Olympic data."""
        jobs = []
        
        # Get storage paths
        storage_config = self.config.get('storage', {})
        raw_path = storage_config.get('paths', {}).get('raw', '')
        processed_path = storage_config.get('paths', {}).get('processed', '')
        
        # Process each data type
        data_types = ['athletes', 'medals', 'teams', 'coaches']
        
        for data_type in data_types:
            input_path = f"{raw_path}/{data_type}"
            output_path = f"{processed_path}/{data_type}"
            
            if data_type == 'athletes':
                job = self.processor.process_athletes_data(input_path, output_path)
            elif data_type == 'medals':
                job = self.processor.process_medals_data(input_path, output_path)
            elif data_type == 'teams':
                job = self.processor.process_teams_data(input_path, output_path)
            else:
                continue
            
            jobs.append(job)
        
        self.logger.logger.info(
            "Data transformation jobs submitted",
            job_count=len(jobs),
            data_types=data_types
        )
        
        return jobs
    
    def get_transformation_status(self) -> Dict[str, Any]:
        """Get status of all transformation jobs."""
        jobs = self.processor.get_all_jobs()
        
        status_counts = {
            'pending': 0,
            'running': 0,
            'completed': 0,
            'failed': 0
        }
        
        for job in jobs:
            status_counts[job.status] += 1
        
        return {
            'total_jobs': len(jobs),
            'status_counts': status_counts,
            'jobs': [asdict(job) for job in jobs]
        } 
