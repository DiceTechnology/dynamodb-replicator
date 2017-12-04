package com.imggaming.dce.replicator;

import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.lambda.runtime.Context;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.List;

public class ReplicationTargets {

    private static ReplicationTargetsImpl client;

    public static ReplicationTargetsImpl getInstance(Context context) {
        if (client == null) {
            client = new ReplicationTargetsImpl(context);
        }
        return client;
    }

    // To consider - can a single lambda handle multiple regions - retry depends on an
    // exception being thrown back from the handleRequest method????
    public static final class ReplicationTargetsImpl {

        private static List<Table> targets = new ArrayList<>();
        private static boolean isSingleTarget = true;
        private static Logger logger;

        private static final String REPLICATED = "Replicated";
        private static final String LOG_LEVEL = "LOG_LEVEL";
        private static final String TARGET_TABLE = "TARGET_TABLE";
        private static final String TARGET_REGIONS = "TARGET_REGIONS";

        private ReplicationTargetsImpl(Context context) {
            logger = Logger.getLogger(ReplicationTargetsImpl.class);

            String loglevel = System.getenv(LOG_LEVEL);
            logger.setLevel(Level.toLevel(loglevel, Level.DEBUG));

            try {
                // All target tables must have the same name.
                String targetTable = System.getenv(TARGET_TABLE);

                logger.info("Target table is [" + targetTable + "]");

                // Target regions are a comma-seperated string.
                String targetRegions = System.getenv(TARGET_REGIONS);
                String[] regions = targetRegions.split(",");

                List<Regions> regionList = new ArrayList<>();

                for (String region : regions) {
                    Regions targetRegion = Regions.valueOf(region);
                    if (targetRegion != null) {
                        regionList.add(targetRegion);
                        logger.info("Added [" + region + "] to list of targets");
                    }
                }

                for (Regions region : regionList) {
                    AmazonDynamoDB client = AmazonDynamoDBClientBuilder.standard()
                            .withRegion(region)
                            .build();

                    DynamoDB dynamoDB = new DynamoDB(client);
                    targets.add(dynamoDB.getTable(targetTable));
                }

                // Record whether we are replicating to a single region.
                isSingleTarget = regionList.size() < 2;
                logger.info("Built " + regionList.size() + " target regions for replication.");
            } catch (Throwable t) {
                logger.error("Error initialising lambda :" + t.getMessage());
            }
        }

        public void replicateChanges(List<ItemOperation> operations) {
            for (ItemOperation operation: operations) {

                // Updates/Inserts...
                if (operation.isUpdate()) {
                    Item item = operation.getItem();
                    // So... for every item we need to know whether this is a new row or one that we have
                    // already replicated from another table, in which case we are not interested and will
                    // ignore it.

                    if (!item.isPresent(REPLICATED))
                    {
                        // Set the value - we'll ignore this item in other streams.
                        item.withBoolean(REPLICATED, true);

                        for (Table target : targets) {
                            // Catch errors per target - we don't want a missing region to prevent us from
                            // writing to all of the other ones.
                            try {
                                logger.debug("Writing a record... '" + item.toJSON() + "' to [" + target.getTableName() + "].");
                                target.putItem(item);
                            } catch (Throwable t) {
                                logger.error("Error writing record '" + item.toJSON() + "' :" + t.getMessage());

                                // Propagate the error if we are replicating to a single region. In these circumstances
                                // we would like the stream to send the event to us again for retry.
                                if (isSingleTarget) {
                                    throw t;
                                }
                            }
                        }
                    }
                } else if (operation.isDelete()) {

                    for (Table target : targets) {
                        try {
                            logger.debug("Deleting record... with PK = '" + operation.getPKValue() + "' from [" + target.getTableName() + "].");
                            target.deleteItem(operation.getPKName(), operation.getPKValue());
                        } catch (Throwable t) {
                            logger.error("Error deleting record with PK = '" + operation.getPKValue() + "' :" + t.getMessage());

                            // Propagate the error if we are replicating to a single region. In these circumstances
                            // we would like the stream to send teh event to us again for retry.
                            if (isSingleTarget) {
                                throw t;
                            }
                        }
                    }
                }
            }
        }

        public Logger getLogger() {
            return logger;
        }
    }
}