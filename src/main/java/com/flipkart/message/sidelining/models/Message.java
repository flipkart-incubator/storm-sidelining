package com.flipkart.message.sidelining.models;


import com.flipkart.message.sidelining.dao.HBaseDAO;

/**
 * Created by saurabh.jha on 15/09/16.
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

    public String getRowKey() { return HBaseDAO.getRowKey(topic,groupId);}


}
