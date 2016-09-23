package com.flipkart.message.sidelining.configs;

/**
 * Created by saurabh.jha on 19/09/16.
 */
public class HBaseTableConfig {
    //table name
    public static final String TABLE_NAME = "oms.messages";

    //column family names
    public static final String COL_FAMILY_ATTRIBUTES = "attributes";
    public static final String COL_FAMILY_DATA = "data";

    //attributes name
    public static final String ATTR_TOPIC = "topic";
    public static final String ATTR_GROUPID = "groupId";
}
