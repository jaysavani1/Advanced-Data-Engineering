"""
Apache Kafka Producer for Olympic Analytics Platform.
Handles data streaming to Kafka topics with error handling and monitoring.
"""

import json
import time
import asyncio
from datetime import datetime
from typing import Dict, Any, List, Optional, Callable
from dataclasses import dataclass, asdict
from enum import Enum

from kafka import KafkaProducer
from kafka.errors import KafkaError, KafkaTimeoutError
from confluent_kafka import Producer as ConfluentProducer
from confluent_kafka.admin import AdminClient, NewTopic

from ..utils.config import get_config
from ..utils.logger import PipelineLogger


class MessageType(Enum):
    """Types of messages that can be sent to Kafka."""
    ATHLETE_DATA = "athlete_data"
    MEDAL_DATA = "medal_data"
    TEAM_DATA = "team_data"
    EVENT_DATA = "event_data"
    COACH_DATA = "coach_data"
    PERFORMANCE_DATA = "performance_data"


@dataclass
class KafkaMessage:
    """Represents a message to be sent to Kafka."""
    message_type: MessageType
    data: Dict[str, Any]
    timestamp: datetime
    source: str
    version: str = "1.0"
    metadata: Optional[Dict[str, Any]] = None
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert message to dictionary for serialization."""
        return {
            'type': self.message_type.value,
            'data': self.data,
            'timestamp': self.timestamp.isoformat(),
            'source': self.source,
            'version': self.version,
            'metadata': self.metadata or {}
        }


class KafkaProducerManager:
    """Manages Kafka producer with error handling and monitoring."""
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize Kafka producer manager.
        
        Args:
            config: Kafka configuration
        """
        self.config = config
        self.kafka_config = config.get('kafka', {})
        self.producer = None
        self.logger = PipelineLogger("kafka_producer", config.get('logging', {}))
        self.sent_messages = 0
        self.failed_messages = 0
        self.is_connected = False
        
        self._setup_producer()
    
    def _setup_producer(self) -> None:
        """Set up Kafka producer with configuration."""
        try:
            # Get Kafka configuration
            bootstrap_servers = self.kafka_config.get('bootstrap_servers', 'localhost:9092')
            producer_config = self.kafka_config.get('producer', {})
            
            # Configure producer
            producer_config_dict = {
                'bootstrap_servers': bootstrap_servers,
                'acks': producer_config.get('acks', 'all'),
                'retries': producer_config.get('retries', 3),
                'batch_size': producer_config.get('batch_size', 16384),
                'linger_ms': producer_config.get('linger_ms', 10),
                'compression_type': producer_config.get('compression_type', 'gzip'),
                'key_serializer': lambda k: k.encode('utf-8') if k else None,
                'value_serializer': lambda v: json.dumps(v, default=str).encode('utf-8')
            }
            
            self.producer = KafkaProducer(**producer_config_dict)
            
            # Test connection
            self._test_connection()
            
            self.is_connected = True
            self.logger.logger.info(
                "Kafka producer initialized",
                bootstrap_servers=bootstrap_servers,
                producer_config=producer_config
            )
            
        except Exception as e:
            self.logger.pipeline_error(e, setup_phase=True)
            raise
    
    def _test_connection(self) -> None:
        """Test Kafka connection."""
        try:
            # Send a test message to verify connection
            future = self.producer.send('test-topic', {'test': 'connection'})
            future.get(timeout=10)
            self.logger.logger.info("Kafka connection test successful")
        except Exception as e:
            self.logger.logger.warning(f"Kafka connection test failed: {e}")
    
    def send_message(self, message: KafkaMessage, topic: Optional[str] = None) -> bool:
        """
        Send a message to Kafka topic.
        
        Args:
            message: KafkaMessage to send
            topic: Target topic (uses default if not specified)
            
        Returns:
            True if message was sent successfully, False otherwise
        """
        if not self.is_connected or not self.producer:
            self.logger.logger.error("Producer not connected")
            return False
        
        try:
            # Determine topic
            target_topic = topic or self._get_topic_for_message_type(message.message_type)
            
            # Convert message to dictionary
            message_dict = message.to_dict()
            
            # Generate key for partitioning
            key = self._generate_message_key(message)
            
            # Send message
            future = self.producer.send(
                topic=target_topic,
                key=key,
                value=message_dict
            )
            
            # Wait for send result
            record_metadata = future.get(timeout=10)
            
            self.sent_messages += 1
            
            self.logger.logger.debug(
                "Message sent successfully",
                topic=target_topic,
                partition=record_metadata.partition,
                offset=record_metadata.offset,
                message_type=message.message_type.value
            )
            
            return True
            
        except KafkaTimeoutError as e:
            self.failed_messages += 1
            self.logger.logger.error(
                "Kafka send timeout",
                error=str(e),
                message_type=message.message_type.value,
                topic=target_topic
            )
            return False
            
        except KafkaError as e:
            self.failed_messages += 1
            self.logger.logger.error(
                "Kafka send error",
                error=str(e),
                message_type=message.message_type.value,
                topic=target_topic
            )
            return False
            
        except Exception as e:
            self.failed_messages += 1
            self.logger.pipeline_error(
                e,
                message_type=message.message_type.value,
                topic=target_topic
            )
            return False
    
    def send_batch(self, messages: List[KafkaMessage], topic: Optional[str] = None) -> Dict[str, int]:
        """
        Send a batch of messages to Kafka.
        
        Args:
            messages: List of KafkaMessage objects
            topic: Target topic (uses default if not specified)
            
        Returns:
            Dictionary with success and failure counts
        """
        success_count = 0
        failure_count = 0
        
        for message in messages:
            if self.send_message(message, topic):
                success_count += 1
            else:
                failure_count += 1
        
        self.logger.data_processed(
            len(messages),
            success_count=success_count,
            failure_count=failure_count,
            topic=topic
        )
        
        return {
            'success_count': success_count,
            'failure_count': failure_count,
            'total_count': len(messages)
        }
    
    def _get_topic_for_message_type(self, message_type: MessageType) -> str:
        """Get the appropriate topic for a message type."""
        topic_mapping = {
            MessageType.ATHLETE_DATA: self.kafka_config.get('topic_olympics', 'olympics-data'),
            MessageType.MEDAL_DATA: self.kafka_config.get('topic_olympics', 'olympics-data'),
            MessageType.TEAM_DATA: self.kafka_config.get('topic_olympics', 'olympics-data'),
            MessageType.EVENT_DATA: self.kafka_config.get('topic_events', 'olympic-events'),
            MessageType.COACH_DATA: self.kafka_config.get('topic_olympics', 'olympics-data'),
            MessageType.PERFORMANCE_DATA: self.kafka_config.get('topic_events', 'olympic-events')
        }
        
        return topic_mapping.get(message_type, self.kafka_config.get('topic_olympics', 'olympics-data'))
    
    def _generate_message_key(self, message: KafkaMessage) -> str:
        """Generate a key for message partitioning."""
        if message.message_type == MessageType.ATHLETE_DATA:
            return message.data.get('country', 'unknown')
        elif message.message_type == MessageType.MEDAL_DATA:
            return message.data.get('country', 'unknown')
        elif message.message_type == MessageType.TEAM_DATA:
            return message.data.get('country', 'unknown')
        elif message.message_type == MessageType.EVENT_DATA:
            return message.data.get('discipline', 'unknown')
        else:
            return 'default'
    
    def create_athlete_message(self, athlete_data: Dict[str, Any]) -> KafkaMessage:
        """Create an athlete data message."""
        return KafkaMessage(
            message_type=MessageType.ATHLETE_DATA,
            data=athlete_data,
            timestamp=datetime.utcnow(),
            source='olympic_analytics_platform'
        )
    
    def create_medal_message(self, medal_data: Dict[str, Any]) -> KafkaMessage:
        """Create a medal data message."""
        return KafkaMessage(
            message_type=MessageType.MEDAL_DATA,
            data=medal_data,
            timestamp=datetime.utcnow(),
            source='olympic_analytics_platform'
        )
    
    def create_team_message(self, team_data: Dict[str, Any]) -> KafkaMessage:
        """Create a team data message."""
        return KafkaMessage(
            message_type=MessageType.TEAM_DATA,
            data=team_data,
            timestamp=datetime.utcnow(),
            source='olympic_analytics_platform'
        )
    
    def create_event_message(self, event_data: Dict[str, Any]) -> KafkaMessage:
        """Create an event data message."""
        return KafkaMessage(
            message_type=MessageType.EVENT_DATA,
            data=event_data,
            timestamp=datetime.utcnow(),
            source='olympic_analytics_platform'
        )
    
    def flush(self) -> None:
        """Flush all pending messages."""
        if self.producer:
            self.producer.flush()
            self.logger.logger.info("Producer flushed")
    
    def close(self) -> None:
        """Close the producer connection."""
        if self.producer:
            self.producer.close()
            self.is_connected = False
            self.logger.logger.info("Producer closed")
    
    def get_stats(self) -> Dict[str, Any]:
        """Get producer statistics."""
        return {
            'sent_messages': self.sent_messages,
            'failed_messages': self.failed_messages,
            'success_rate': self.sent_messages / max(self.sent_messages + self.failed_messages, 1),
            'is_connected': self.is_connected
        }


class DataStreamer:
    """Streams Olympic data to Kafka topics."""
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize data streamer.
        
        Args:
            config: Configuration dictionary
        """
        self.config = config
        self.producer_manager = KafkaProducerManager(config)
        self.logger = PipelineLogger("data_streamer", config.get('logging', {}))
    
    def stream_athletes_data(self, athletes_data: List[Dict[str, Any]]) -> None:
        """Stream athletes data to Kafka."""
        messages = []
        for athlete in athletes_data:
            message = self.producer_manager.create_athlete_message(athlete)
            messages.append(message)
        
        result = self.producer_manager.send_batch(messages)
        self.logger.data_processed(
            len(athletes_data),
            data_type='athletes',
            **result
        )
    
    def stream_medals_data(self, medals_data: List[Dict[str, Any]]) -> None:
        """Stream medals data to Kafka."""
        messages = []
        for medal in medals_data:
            message = self.producer_manager.create_medal_message(medal)
            messages.append(message)
        
        result = self.producer_manager.send_batch(messages)
        self.logger.data_processed(
            len(medals_data),
            data_type='medals',
            **result
        )
    
    def stream_teams_data(self, teams_data: List[Dict[str, Any]]) -> None:
        """Stream teams data to Kafka."""
        messages = []
        for team in teams_data:
            message = self.producer_manager.create_team_message(team)
            messages.append(message)
        
        result = self.producer_manager.send_batch(messages)
        self.logger.data_processed(
            len(teams_data),
            data_type='teams',
            **result
        )
    
    def stream_events_data(self, events_data: List[Dict[str, Any]]) -> None:
        """Stream events data to Kafka."""
        messages = []
        for event in events_data:
            message = self.producer_manager.create_event_message(event)
            messages.append(message)
        
        result = self.producer_manager.send_batch(messages)
        self.logger.data_processed(
            len(events_data),
            data_type='events',
            **result
        )
    
    def close(self) -> None:
        """Close the data streamer."""
        self.producer_manager.close()


def main():
    """Main function to demonstrate Kafka producer usage."""
    # Load configuration
    config = get_config().to_dict()
    
    # Create data streamer
    streamer = DataStreamer(config)
    
    try:
        # Example: Stream sample data
        sample_athletes = [
            {
                'name': 'John Doe',
                'country': 'USA',
                'discipline': 'Athletics',
                'age': 25
            },
            {
                'name': 'Jane Smith',
                'country': 'Canada',
                'discipline': 'Swimming',
                'age': 23
            }
        ]
        
        sample_medals = [
            {
                'country': 'USA',
                'medal_type': 'Gold',
                'event': '100m Sprint',
                'athlete': 'John Doe'
            },
            {
                'country': 'Canada',
                'medal_type': 'Silver',
                'event': '200m Freestyle',
                'athlete': 'Jane Smith'
            }
        ]
        
        # Stream data
        streamer.stream_athletes_data(sample_athletes)
        streamer.stream_medals_data(sample_medals)
        
        # Flush and close
        streamer.producer_manager.flush()
        
    except KeyboardInterrupt:
        print("Shutting down...")
    finally:
        streamer.close()


if __name__ == "__main__":
    main() 