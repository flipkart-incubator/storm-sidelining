package com.flipkart.storm.sidelining.core.spout;

import com.flipkart.storm.sidelining.core.spout.model.GroupedEvents;
import com.flipkart.storm.sidelining.core.spout.model.PartitionState;

import java.util.Queue;

/**
 * Created by gupta.rajat on 27/07/17.
 */
public interface PartitionManager {

    PartitionState fillEmitQueue(Queue<GroupedEvents> toEmitEvents);

}