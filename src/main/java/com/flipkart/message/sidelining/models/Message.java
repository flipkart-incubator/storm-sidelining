package com.flipkart.message.sidelining.models;

import lombok.Data;
import org.json.JSONObject;

import java.sql.Timestamp;

/**
 * Created by saurabh.jha on 15/09/16.
 */
@Data
public class Message {
    String id;
    String groupId;
    String topic;
    byte[] data;
}
