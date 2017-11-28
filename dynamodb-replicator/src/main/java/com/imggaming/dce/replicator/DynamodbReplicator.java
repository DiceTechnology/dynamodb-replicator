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

    private static final String INSERT = "INSERT";
    private static final String MODIFY = "MODIFY";

    @Override
    public Void handleRequest(DynamodbEvent dynamodbEvent, Context context) {

        ReplicationTargets.ReplicationTargetsImpl targets = ReplicationTargets.getInstance(context);
        Logger logger = targets.getLogger();

        try {
            logger.debug("DynamodbReplicator.handleRequest() invoked");

            List<Item> items = new ArrayList<>();
            List<Map<String, AttributeValue>> recordImages = null;

            for (DynamodbEvent.DynamodbStreamRecord record : dynamodbEvent.getRecords()) {

                logger.debug("Got a record...");
                if (record == null) {
                    continue;
                }

                if (INSERT.equals(record.getEventName()) || MODIFY.equals(record.getEventName())) {
                    recordImages = new ArrayList<>();
                    recordImages.add(record.getDynamodb().getNewImage());
                    items = InternalUtils.toItemList(recordImages);
                }

                if (!items.isEmpty()) {
                    targets.replicateItems(items);
                }
            }

            logger.debug("DynamodbReplicator.handleRequest() completed.");
        } catch (Throwable t){
            logger.error("Exception: " + t.getMessage());
            throw t;
        }

        return null;
    }
}

