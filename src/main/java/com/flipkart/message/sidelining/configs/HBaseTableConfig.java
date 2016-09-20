package com.flipkart.message.sidelining.configs;

/**
 * Created by saurabh.jha on 19/09/16.
 */
public class HBaseTableConfig {
    public static final String TABLE_NAME = "oms.messages";
    //column family names
    public static final String COL_FAMILY_ATTRIBUTES = "attributes";
    public static final String COL_FAMILY_DATA = "data";
}
