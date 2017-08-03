package com.flipkart.storm.sidelining.core.spout.model;

public class StormMsgId {
    public String rowKey;
    public String messageId;

    public StormMsgId(String rowKey, String messageId) {
        this.rowKey = rowKey;
        this.messageId = messageId;
    }
}