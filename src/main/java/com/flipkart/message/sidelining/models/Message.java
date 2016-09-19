package com.flipkart.message.sidelining.models;

import lombok.Data;
import org.json.JSONObject;

import java.sql.Timestamp;

/**
 * Created by saurabh.jha on 15/09/16.
 */
@Data
public class Message {

    public static final String TABLE_NAME = "oms.messages";
    //column family names
    public static final String COL_FAMILY_ATTRIBUTES = "attributes";
    public static final String COL_FAMILY_DATA = "data";
    public static final String COL_FAMILY_INFO = "info";

    String id;
    String groupId;
    String topic;
    byte[] data;
}
