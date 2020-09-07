package com.jp.eventhub.listener;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.azure.messaging.eventhubs.EventProcessorClient;
import com.azure.messaging.eventhubs.EventProcessorClientBuilder;

@Component
public class EventHubListener {
	
	@Autowired
	private EventProcessorClientBuilder  clientBuilder;
	
	EventProcessorClient eventProcessorClient;
	
	@PostConstruct
	public void listen() {
		eventProcessorClient = clientBuilder.buildEventProcessorClient();
		eventProcessorClient.start();
	}
	
	@PreDestroy
	public void stop() {
		eventProcessorClient.stop();
	}

}
