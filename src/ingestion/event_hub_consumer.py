"""
Azure Event Hub Consumer for Olympic Analytics Platform.
Handles real-time data ingestion from Event Hubs with checkpointing and error handling.
"""

import asyncio
import json
import time
from datetime import datetime
from typing import Dict, Any, Optional, Callable
from dataclasses import dataclass

from azure.eventhub import EventHubConsumerClient
from azure.eventhub.checkpointstoreblob import BlobCheckpointStore
from azure.storage.blob import BlobServiceClient
from azure.identity import DefaultAzureCredential

from ..utils.config import get_config
from ..utils.logger import PipelineLogger


@dataclass
class EventHubMessage:
    """Represents a message from Event Hub."""
    body: Dict[str, Any]
    partition_id: str
    sequence_number: int
    offset: int
    enqueued_time: datetime
    properties: Dict[str, Any]


class EventHubConsumer:
    """Azure Event Hub consumer with checkpointing and error handling."""
    
    def __init__(self, config: Dict[str, Any], message_processor: Optional[Callable] = None):
        """
        Initialize Event Hub consumer.
        
        Args:
            config: Event Hub configuration
            message_processor: Optional custom message processor function
        """
        self.config = config
        self.message_processor = message_processor
        self.consumer_client = None
        self.checkpoint_store = None
        self.logger = PipelineLogger("event_hub_consumer", config.get('logging', {}))
        self.is_running = False
        self.processed_messages = 0
        self.error_count = 0
        
        # Initialize Azure credentials
        self.credential = DefaultAzureCredential()
        
        self._setup_consumer()
    
    def _setup_consumer(self) -> None:
        """Set up Event Hub consumer client and checkpoint store."""
        try:
            # Get Event Hub configuration
            event_hub_config = self.config.get('event_hubs', {})
            storage_config = self.config.get('storage', {})
            
            # Set up checkpoint store
            checkpoint_connection_string = storage_config.get('account_key')
            checkpoint_container = storage_config.get('containers', {}).get('checkpoints', 'checkpoints')
            
            self.checkpoint_store = BlobCheckpointStore(
                blob_service_client=BlobServiceClient.from_connection_string(
                    checkpoint_connection_string
                ),
                container_name=checkpoint_container
            )
            
            # Set up consumer client
            self.consumer_client = EventHubConsumerClient(
                fully_qualified_namespace=event_hub_config.get('namespace'),
                eventhub_name=event_hub_config.get('hubs', {}).get('olympics_data', {}).get('name', 'olympics-data'),
                consumer_group=event_hub_config.get('hubs', {}).get('olympics_data', {}).get('consumer_group', '$Default'),
                credential=self.credential,
                checkpoint_store=self.checkpoint_store
            )
            
            self.logger.logger.info(
                "Event Hub consumer initialized",
                namespace=event_hub_config.get('namespace'),
                eventhub_name=event_hub_config.get('hubs', {}).get('olympics_data', {}).get('name'),
                consumer_group=event_hub_config.get('hubs', {}).get('olympics_data', {}).get('consumer_group')
            )
            
        except Exception as e:
            self.logger.pipeline_error(e, setup_phase=True)
            raise
    
    async def on_event(self, partition_context, event) -> None:
        """
        Process incoming events from Event Hub.
        
        Args:
            partition_context: Partition context for checkpointing
            event: Event data
        """
        try:
            # Parse event data
            event_data = json.loads(event.body_as_str())
            
            # Create message object
            message = EventHubMessage(
                body=event_data,
                partition_id=partition_context.partition_id,
                sequence_number=event.sequence_number,
                offset=event.offset,
                enqueued_time=event.enqueued_time,
                properties=event.properties
            )
            
            # Process message
            await self._process_message(message)
            
            # Update checkpoint
            await partition_context.update_checkpoint(event)
            
            self.processed_messages += 1
            
            # Log progress periodically
            if self.processed_messages % 100 == 0:
                self.logger.data_processed(
                    self.processed_messages,
                    partition_id=partition_context.partition_id,
                    error_count=self.error_count
                )
                
        except json.JSONDecodeError as e:
            self.error_count += 1
            self.logger.logger.error(
                "Failed to parse JSON from event",
                error=str(e),
                partition_id=partition_context.partition_id,
                sequence_number=event.sequence_number
            )
            
        except Exception as e:
            self.error_count += 1
            self.logger.pipeline_error(
                e,
                partition_id=partition_context.partition_id,
                sequence_number=event.sequence_number
            )
    
    async def _process_message(self, message: EventHubMessage) -> None:
        """
        Process individual message.
        
        Args:
            message: Event Hub message
        """
        try:
            # Use custom processor if provided
            if self.message_processor:
                await self.message_processor(message)
            else:
                # Default processing logic
                await self._default_message_processor(message)
                
        except Exception as e:
            self.logger.logger.error(
                "Failed to process message",
                error=str(e),
                partition_id=message.partition_id,
                sequence_number=message.sequence_number
            )
            raise
    
    async def _default_message_processor(self, message: EventHubMessage) -> None:
        """
        Default message processing logic.
        
        Args:
            message: Event Hub message
        """
        # Extract message type and route accordingly
        message_type = message.body.get('type', 'unknown')
        
        if message_type == 'athlete_data':
            await self._process_athlete_data(message.body)
        elif message_type == 'medal_data':
            await self._process_medal_data(message.body)
        elif message_type == 'team_data':
            await self._process_team_data(message.body)
        elif message_type == 'event_data':
            await self._process_event_data(message.body)
        else:
            self.logger.logger.warning(
                "Unknown message type",
                message_type=message_type,
                partition_id=message.partition_id
            )
    
    async def _process_athlete_data(self, data: Dict[str, Any]) -> None:
        """Process athlete data."""
        # Validate required fields
        required_fields = ['name', 'country', 'discipline']
        if not all(field in data for field in required_fields):
            raise ValueError(f"Missing required fields: {required_fields}")
        
        # Process athlete data (e.g., save to storage, trigger analytics)
        self.logger.logger.info(
            "Processing athlete data",
            athlete_name=data.get('name'),
            country=data.get('country'),
            discipline=data.get('discipline')
        )
    
    async def _process_medal_data(self, data: Dict[str, Any]) -> None:
        """Process medal data."""
        # Validate required fields
        required_fields = ['country', 'medal_type', 'event']
        if not all(field in data for field in required_fields):
            raise ValueError(f"Missing required fields: {required_fields}")
        
        # Process medal data
        self.logger.logger.info(
            "Processing medal data",
            country=data.get('country'),
            medal_type=data.get('medal_type'),
            event=data.get('event')
        )
    
    async def _process_team_data(self, data: Dict[str, Any]) -> None:
        """Process team data."""
        # Validate required fields
        required_fields = ['team_name', 'country', 'discipline']
        if not all(field in data for field in required_fields):
            raise ValueError(f"Missing required fields: {required_fields}")
        
        # Process team data
        self.logger.logger.info(
            "Processing team data",
            team_name=data.get('team_name'),
            country=data.get('country'),
            discipline=data.get('discipline')
        )
    
    async def _process_event_data(self, data: Dict[str, Any]) -> None:
        """Process event data."""
        # Validate required fields
        required_fields = ['event_name', 'discipline', 'date']
        if not all(field in data for field in required_fields):
            raise ValueError(f"Missing required fields: {required_fields}")
        
        # Process event data
        self.logger.logger.info(
            "Processing event data",
            event_name=data.get('event_name'),
            discipline=data.get('discipline'),
            date=data.get('date')
        )
    
    async def start_consuming(self) -> None:
        """Start consuming events from Event Hub."""
        if not self.consumer_client:
            raise RuntimeError("Consumer client not initialized")
        
        self.is_running = True
        start_time = time.time()
        
        self.logger.start_pipeline(
            consumer_group=self.config.get('event_hubs', {}).get('hubs', {}).get('olympics_data', {}).get('consumer_group')
        )
        
        try:
            async with self.consumer_client:
                await self.consumer_client.receive(
                    on_event=self.on_event,
                    starting_position="-1",  # Start from latest event
                    track_last_enqueued_event_properties=True
                )
                
        except KeyboardInterrupt:
            self.logger.logger.info("Received interrupt signal, stopping consumer")
        except Exception as e:
            self.logger.pipeline_error(e)
            raise
        finally:
            self.is_running = False
            duration = time.time() - start_time
            
            self.logger.end_pipeline(
                duration,
                processed_messages=self.processed_messages,
                error_count=self.error_count
            )
    
    async def stop_consuming(self) -> None:
        """Stop consuming events."""
        self.is_running = False
        if self.consumer_client:
            await self.consumer_client.close()
    
    def get_stats(self) -> Dict[str, Any]:
        """Get consumer statistics."""
        return {
            'processed_messages': self.processed_messages,
            'error_count': self.error_count,
            'is_running': self.is_running,
            'success_rate': (self.processed_messages - self.error_count) / max(self.processed_messages, 1)
        }


class EventHubConsumerManager:
    """Manages multiple Event Hub consumers."""
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize consumer manager.
        
        Args:
            config: Configuration dictionary
        """
        self.config = config
        self.consumers = {}
        self.logger = PipelineLogger("event_hub_manager", config.get('logging', {}))
    
    def create_consumer(self, name: str, message_processor: Optional[Callable] = None) -> EventHubConsumer:
        """
        Create a new Event Hub consumer.
        
        Args:
            name: Consumer name
            message_processor: Optional custom message processor
            
        Returns:
            EventHubConsumer instance
        """
        consumer = EventHubConsumer(self.config, message_processor)
        self.consumers[name] = consumer
        return consumer
    
    async def start_all_consumers(self) -> None:
        """Start all registered consumers."""
        tasks = []
        for name, consumer in self.consumers.items():
            task = asyncio.create_task(consumer.start_consuming())
            tasks.append(task)
        
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
    
    async def stop_all_consumers(self) -> None:
        """Stop all registered consumers."""
        for consumer in self.consumers.values():
            await consumer.stop_consuming()
    
    def get_all_stats(self) -> Dict[str, Dict[str, Any]]:
        """Get statistics for all consumers."""
        return {name: consumer.get_stats() for name, consumer in self.consumers.items()}


async def main():
    """Main function to run the Event Hub consumer."""
    # Load configuration
    config = get_config().to_dict()
    
    # Create consumer manager
    manager = EventHubConsumerManager(config)
    
    # Create consumer
    consumer = manager.create_consumer("olympics_consumer")
    
    try:
        # Start consuming
        await manager.start_all_consumers()
    except KeyboardInterrupt:
        print("Shutting down...")
    finally:
        await manager.stop_all_consumers()


if __name__ == "__main__":
    asyncio.run(main()) 
