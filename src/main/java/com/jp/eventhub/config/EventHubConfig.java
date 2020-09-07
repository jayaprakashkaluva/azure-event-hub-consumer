package com.jp.eventhub.config;

import java.util.function.Consumer;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.azure.messaging.eventhubs.EventHubClientBuilder;
import com.azure.messaging.eventhubs.EventProcessorClientBuilder;
import com.azure.messaging.eventhubs.checkpointstore.blob.BlobCheckpointStore;
import com.azure.messaging.eventhubs.models.ErrorContext;
import com.azure.messaging.eventhubs.models.EventContext;
import com.azure.storage.blob.BlobContainerAsyncClient;
import com.azure.storage.blob.BlobContainerClientBuilder;

@Configuration
public class EventHubConfig {
	
	public static final Consumer<EventContext> PARTITION_PROCESSOR = eventContext -> {
	     System.out.printf("------------Processing event from partition %s with sequence number %d with body: %s %n", 
	             eventContext.getPartitionContext().getPartitionId(), eventContext.getEventData().getSequenceNumber(), eventContext.getEventData().getBodyAsString());
	     System.out.println("--------------------"+eventContext.getEventData().getBodyAsString());
	    if (eventContext.getEventData().getSequenceNumber() % 10 == 0) {
	        eventContext.updateCheckpoint();
	    }
	};

	public static final Consumer<ErrorContext> ERROR_HANDLER = errorContext -> {
	    System.out.printf("-------------------Error occurred in partition processor for partition %s, %s.%n",
	        errorContext.getPartitionContext().getPartitionId(),
	        errorContext.getThrowable());
	};

	@Value("${eventhub.connection}")
	private String connectionString;
	@Value("${eventhub.name}")
	private String eventHubName;
	@Value("${storageaccount.connection}")
	private String storageAccountConnection;
	@Value("${storageaccount.name}")
	private String storageContainerName;
	
	
	@Bean
	public EventProcessorClientBuilder  eventProcessorClientBuilder() {
		return  new EventProcessorClientBuilder()
	        .connectionString(connectionString, eventHubName) 
	        .consumerGroup(EventHubClientBuilder.DEFAULT_CONSUMER_GROUP_NAME)
	        .processEvent(PARTITION_PROCESSOR) 
	        .processError(ERROR_HANDLER) 
	        .checkpointStore(new BlobCheckpointStore(blobContainerAsyncClient())); 
	}
	
	@Bean
	public BlobContainerAsyncClient blobContainerAsyncClient() {
		return new BlobContainerClientBuilder()
		        .connectionString(storageAccountConnection) 
		        .containerName(storageContainerName) 
		        .buildAsyncClient();
	}
}
