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

    public static class ReplicationTargetsImpl {

        private static List<Table> targets = new ArrayList<>();
        private static Logger logger;

        private ReplicationTargetsImpl(Context context) {
            logger = Logger.getLogger(ReplicationTargetsImpl.class);

            String loglevel = System.getenv("LOG_LEVEL");
            logger.setLevel(Level.toLevel(loglevel, Level.DEBUG));

            try {
                // All target tables must have the same name.
                String targetTable = System.getenv("TARGET_TABLE");

                logger.info("Target table is [" + targetTable + "]");

                // Target regions are a comma-seperated string.
                String targetRegions = System.getenv("TARGET_REGIONS");
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

                logger.info("Built " + regionList.size() + " target regions for replication.");
            } catch (Throwable t) {
                logger.error("Error initialising lambda :" + t.getMessage());
            }
        }

        public void replicateItems(List<Item> items) {
            for (Item item : items) {
                for (Table target : targets) {
                    // Catch errors per target - we don't want a missing region to prevent us from
                    // writing to all of the other ones.
                    try {
                        logger.debug("Writing a record... '" + item.toJSON() + "' to [" + target.getTableName() + "].");
                        target.putItem(item);
                    } catch (Throwable t) {
                        logger.error("Error writing record '" + item.toJSON() + "' :" + t.getMessage());
                    }
                }
            }
        }

        public Logger getLogger() {
            return logger;
        }
    }
}