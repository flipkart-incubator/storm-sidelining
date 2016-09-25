package com.flipkart.message.sidelining.models;

import lombok.Data;

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
