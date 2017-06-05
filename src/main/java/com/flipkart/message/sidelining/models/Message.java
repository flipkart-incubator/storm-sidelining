package com.flipkart.message.sidelining.models;


import com.flipkart.message.sidelining.dao.HbaseDataStore;

import java.util.Arrays;

/**
 * Created by gupta.rajat on 05/06/17.
 */

public class Message {
    private String id;
    private String groupId;
    private String topic;
    private byte[] data;

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
