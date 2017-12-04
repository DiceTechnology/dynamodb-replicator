package com.imggaming.dce.replicator;

import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.internal.InternalUtils;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.DynamodbEvent;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class DynamodbReplicator implements RequestHandler<DynamodbEvent, Void> {

    @Override
    public Void handleRequest(DynamodbEvent dynamodbEvent, Context context) {

        ReplicationTargets.ReplicationTargetsImpl targets = ReplicationTargets.getInstance(context);
        Logger logger = targets.getLogger();

        try {
            logger.debug("DynamodbReplicator.handleRequest() invoked");

            List<ItemOperation> items = new ArrayList<>();
            List<Map<String, AttributeValue>> recordImages = null;

            for (DynamodbEvent.DynamodbStreamRecord record : dynamodbEvent.getRecords()) {

                if (record == null) {
                    continue;
                }

                String operation = record.getEventName();
                logger.debug("Got a/an " + operation + " record...");

                if (ItemOperation.INSERT.equals(operation) || ItemOperation.MODIFY.equals(operation)) {
                    // For updates we simply want to set the item to the values contained in the new image.
                    recordImages = new ArrayList<>();
                    recordImages.add(record.getDynamodb().getNewImage());
                    Item item = InternalUtils.toItemList(recordImages).get(0);

                    items.add(new ItemOperation(operation, item));

                } else if (ItemOperation.REMOVE.equals(operation)) {
                    // For deletes we need the key attributes.
                    Map<String, AttributeValue> keys = record.getDynamodb().getKeys();
                    items.add(new ItemOperation(operation, keys));
                }
            }

            if (!items.isEmpty()) {
                targets.replicateChanges(items);
            }

            logger.debug("DynamodbReplicator.handleRequest() completed.");
        } catch (Throwable t){
            logger.error("Exception: " + t.getMessage());
            throw t;
        }

        return null;
    }
}