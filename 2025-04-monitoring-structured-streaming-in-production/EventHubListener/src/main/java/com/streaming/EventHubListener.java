package com.streaming;

import java.util.logging.Logger;
import java.util.logging.Level;
import org.apache.spark.sql.streaming.StreamingQueryListener;
import com.azure.messaging.eventhubs.EventHubClientBuilder;
import com.azure.messaging.eventhubs.EventHubProducerClient;
import com.azure.messaging.eventhubs.EventData;
import com.azure.messaging.eventhubs.EventDataBatch;

public class EventHubListener extends StreamingQueryListener {
    private static final String connectionString = "...";
    private static final String eventHubName = "...";
    private static final EventHubProducerClient eventHubProducerClient =
            new EventHubClientBuilder()
                    .connectionString(connectionString, eventHubName)
                    .buildProducerClient();
    private static final Logger logger = Logger.getLogger(EventHubListener.class.getName())

    public void onQueryStarted(StreamingQueryListener.QueryStartedEvent event) {
        /**
         * Called when a structured streaming query is started or resumed.
         * @param event QueryStartedEvent to be processed by the listener.
         */
        try {
            String message = event.toString();
            EventHubListener.postToEventHub(message);
        } catch (Exception e) {
            logger.log(Level.SEVERE, e.getMessage());
        }
    }

    public void onQueryProgress(StreamingQueryListener.QueryProgressEvent event) {
        /**
         * Called when a structured streaming query processes a micro-batch.
         * @param event QueryProgressEvent to be processed by the listener.
         */
        try {
            String message = event.progress().json();
            EventHubListener.postToEventHub(message);
        } catch (Exception e) {
            logger.log(Level.SEVERE, e.getMessage());
        }
    }

    public void onQueryTerminated(StreamingQueryListener.QueryTerminatedEvent event) {
        /**
         * Called when a structured streaming query is terminated.
         * @param event QueryTerminatedEvent to be processed by the listener.
         */
        try {
            String message = event.toString();
            EventHubListener.postToEventHub(message);
        } catch (Exception e) {
            logger.log(Level.SEVERE, e.getMessage());
        }
    }

    private static void postToEventHub(String message) {
        /**
         * Posts a JSON message to Event Hub.
         * @param message A JSON string to be posted.
         */
        EventDataBatch eventDataBatch =
                EventHubListener.eventHubProducerClient.createBatch();
        boolean added = eventDataBatch.tryAdd(new EventData(message));
        if (!added) {
            throw new IllegalArgumentException(
                    String.format("Couldn't add body '%s' to Event Hub message.", message));
        }
        EventHubListener.eventHubProducerClient.send(eventDataBatch);
    }
}