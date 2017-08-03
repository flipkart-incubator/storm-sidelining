package com.flipkart.storm.sidelining.core.sideline;

import backtype.storm.tuple.Tuple;

/**
 * Created by gupta.rajat on 27/07/17.
 */
public interface StormSideliner {
    public boolean sideline(String topic, String groupId, String id, Tuple tuple);
    public boolean groupSideline(String topic, String groupId, String id, Tuple tuple) throws Exception;
}

