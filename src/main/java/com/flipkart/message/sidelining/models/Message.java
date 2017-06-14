package com.flipkart.message.sidelining.models;


import com.flipkart.message.sidelining.dao.HbaseDataStore;

import java.util.Arrays;

/**
 * Created by gupta.rajat on 05/06/17.
 */

public class Message {
    private String topic;
    private String groupId;
    private String id;
    private byte[] data;

    public Message(String topic, String groupId, String id, byte[] data) {
        this.topic = topic;
        this.groupId = groupId;
        this.id = id;
        this.data = data;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getGroupId() {
        return groupId;
    }

    public void setGroupId(String groupId) {
        this.groupId = groupId;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public byte[] getData() {
        return data;
    }

    public void setData(byte[] data) {
        this.data = data;
    }

    public String getRowKey() { return HbaseDataStore.getRowKey(topic,groupId);}

    @Override
    public String toString() {
        return "Message{" +
                "id='" + id + '\'' +
                ", groupId='" + groupId + '\'' +
                ", topic='" + topic + '\'' +
                ", data=" + Arrays.toString(data) +
                '}';
    }
}
