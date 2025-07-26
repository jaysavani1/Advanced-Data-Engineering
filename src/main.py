"""
Main entry point for Olympic Analytics Platform.
Orchestrates the entire data pipeline from ingestion to transformation.
"""

import asyncio
import time
import sys
from datetime import datetime
from typing import Dict, Any, Optional

from .utils.config import get_config
from .utils.logger import PipelineLogger, setup_logging
from .ingestion.event_hub_consumer import EventHubConsumerManager
from .ingestion.kafka_producer import DataStreamer
from .transformation.databricks_processor import DataTransformer
from .quality.data_validator import DataValidator


class OlympicAnalyticsPlatform:
    """Main platform orchestrator for Olympic Analytics."""
    
    def __init__(self, config_path: Optional[str] = None, environment: Optional[str] = None):
        """
        Initialize the Olympic Analytics Platform.
        
        Args:
            config_path: Path to configuration file
            environment: Environment name (dev, staging, prod)
        """
        self.config = get_config(config_path, environment).to_dict()
        self.logger = PipelineLogger("olympic_platform", self.config.get('logging', {}))
        
        # Initialize components
        self.event_hub_manager = EventHubConsumerManager(self.config)
        self.data_streamer = DataStreamer(self.config)
        self.data_transformer = DataTransformer(self.config)
        self.data_validator = DataValidator(self.config)
        
        self.is_running = False
    
    async def start_pipeline(self) -> None:
        """Start the complete data pipeline."""
        self.is_running = True
        start_time = time.time()
        
        self.logger.start_pipeline(
            platform_version=self.config.get('version', '1.0.0'),
            environment=self.config.get('environment', 'dev')
        )
        
        try:
            # Start data ingestion
            await self._start_data_ingestion()
            
            # Process and transform data
            await self._process_data()
            
            # Validate data quality
            await self._validate_data_quality()
            
            duration = time.time() - start_time
            self.logger.end_pipeline(duration, pipeline_type="complete")
            
        except Exception as e:
            self.logger.pipeline_error(e, pipeline_type="complete")
            raise
        finally:
            self.is_running = False
    
    async def _start_data_ingestion(self) -> None:
        """Start data ingestion from Event Hubs."""
        self.logger.logger.info("Starting data ingestion...")
        
        # Create Event Hub consumer
        consumer = self.event_hub_manager.create_consumer("olympics_consumer")
        
        # Start consuming in background
        ingestion_task = asyncio.create_task(
            self.event_hub_manager.start_all_consumers()
        )
        
        # Let it run for a while (in production, this would run continuously)
        await asyncio.sleep(10)
        
        # Stop ingestion
        await self.event_hub_manager.stop_all_consumers()
        ingestion_task.cancel()
        
        self.logger.logger.info("Data ingestion completed")
    
    async def _process_data(self) -> None:
        """Process and transform data using Databricks."""
        self.logger.logger.info("Starting data processing...")
        
        # Submit transformation jobs
        jobs = self.data_transformer.transform_all_data()
        
        # Wait for jobs to complete (in production, you'd poll for status)
        await asyncio.sleep(5)
        
        # Get transformation status
        status = self.data_transformer.get_transformation_status()
        self.logger.logger.info(
            "Data processing completed",
            job_count=status['total_jobs'],
            completed_jobs=status['status_counts']['completed']
        )
    
    async def _validate_data_quality(self) -> None:
        """Validate data quality."""
        self.logger.logger.info("Starting data quality validation...")
        
        # In a real scenario, you'd load the processed data
        # For now, we'll create sample data for validation
        import pandas as pd
        
        sample_athletes = pd.DataFrame({
            'name': ['John Doe', 'Jane Smith'],
            'country': ['USA', 'Canada'],
            'discipline': ['Athletics', 'Swimming']
        })
        
        sample_medals = pd.DataFrame({
            'country': ['USA', 'Canada'],
            'gold': [10, 5],
            'silver': [8, 6],
            'bronze': [7, 4]
        })
        
        sample_teams = pd.DataFrame({
            'team_name': ['Team USA', 'Team Canada'],
            'country': ['USA', 'Canada'],
            'discipline': ['Athletics', 'Swimming']
        })
        
        # Validate data
        validation_results = self.data_validator.validate_olympic_data(
            sample_athletes, sample_medals, sample_teams
        )
        
        # Generate quality report
        quality_report = self.data_validator.generate_quality_report(validation_results)
        
        self.logger.logger.info(
            "Data quality validation completed",
            overall_quality_score=quality_report['overall_quality']['overall_quality_score']
        )
    
    def get_platform_status(self) -> Dict[str, Any]:
        """Get overall platform status."""
        return {
            'is_running': self.is_running,
            'version': self.config.get('version'),
            'environment': self.config.get('environment'),
            'timestamp': datetime.utcnow().isoformat(),
            'components': {
                'event_hub_manager': 'initialized',
                'data_streamer': 'initialized',
                'data_transformer': 'initialized',
                'data_validator': 'initialized'
            }
        }
    
    async def shutdown(self) -> None:
        """Shutdown the platform gracefully."""
        self.logger.logger.info("Shutting down Olympic Analytics Platform...")
        
        # Stop all components
        await self.event_hub_manager.stop_all_consumers()
        self.data_streamer.close()
        
        self.is_running = False
        self.logger.logger.info("Platform shutdown completed")


async def main():
    """Main function to run the Olympic Analytics Platform."""
    # Set up logging
    config = get_config().to_dict()
    setup_logging(config.get('logging', {}))
    
    # Create platform instance
    platform = OlympicAnalyticsPlatform()
    
    try:
        # Start the platform
        await platform.start_pipeline()
        
        # Get status
        status = platform.get_platform_status()
        print(f"Platform Status: {status}")
        
    except KeyboardInterrupt:
        print("\nReceived interrupt signal...")
    except Exception as e:
        print(f"Platform error: {e}")
        sys.exit(1)
    finally:
        await platform.shutdown()


if __name__ == "__main__":
    asyncio.run(main()) 
