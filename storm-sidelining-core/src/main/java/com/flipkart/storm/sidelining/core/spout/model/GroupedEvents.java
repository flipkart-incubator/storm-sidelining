package com.flipkart.storm.sidelining.core.spout.model;

import com.flipkart.storm.sidelining.core.spout.model.Event;
import com.google.common.collect.Lists;

import java.util.Queue;

/**
 * Created by gupta.rajat on 05/06/17.
 */
public class GroupedEvents {
    public String rowKey;
    public Queue<Event> eventQueue = Lists.newLinkedList();
    public long version;
}