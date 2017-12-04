package com.imggaming.dce.replicator;

import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;

import java.util.Map;

public class ItemOperation
{
    public static final String INSERT = "INSERT";
    public static final String MODIFY = "MODIFY";
    public static final String REMOVE = "REMOVE";

    private String operation;
    private Item item;
    private String pKName;
    private String pKValue;

    public ItemOperation(String pOperation, Item pItem) {
        this(pOperation, pItem, null);
    }

    public ItemOperation(String pOperation, Map<String, AttributeValue> pKeys) {
        this(pOperation, null, pKeys);
    }

    public ItemOperation(String pOperation, Item pItem, Map<String, AttributeValue> pKeys)
    {
        operation = pOperation;
        item = pItem;

        // We can only handle a single column String type PK so far. Further investigation of the
        // somewhat abstruse JAVA API will be required to make this more generic.
        if (pKeys != null) {
            Map.Entry<String, AttributeValue> key = pKeys.entrySet().iterator().next();
            pKName = key.getKey();
            pKValue = key.getValue().getS();
        }
    }

    public Item getItem() {
        return item;
    }

    public String getPKName() {
        return pKName;
    }

    public String getPKValue() {
        return pKValue;
    }

    public boolean isUpdate() {
        return operation.equals(INSERT) || operation.equals(MODIFY);
    }

    public boolean isDelete() {
        return operation.equals(REMOVE);
    }
}