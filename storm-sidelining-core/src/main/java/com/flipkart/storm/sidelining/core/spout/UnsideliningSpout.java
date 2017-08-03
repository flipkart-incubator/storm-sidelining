package com.flipkart.storm.sidelining.core.spout;

import backtype.storm.spout.MultiScheme;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.topology.base.BaseRichSpout;
import com.flipkart.storm.sidelining.core.spout.model.GroupedEvents;
import com.flipkart.storm.sidelining.core.spout.model.PartitionState;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.inject.Provider;

import java.util.List;
import java.util.Map;
import java.util.Queue;

/**
 * Created by gupta.rajat on 27/07/17.
 */
public abstract class UnsideliningSpout extends BaseRichSpout {

    protected String unsidelineStream;

    protected String topologyName;

    protected List<PartitionManager> partitionManagers = Lists.newArrayList();
    protected int currentPartitionIndex = 0;
    protected PartitionState currentPartitionState = PartitionState.SCAN_DONE;

    protected Provider<? extends StormUnsideliner> sidelineProvider;
    protected StormUnsideliner stormUnsideliner;
    protected MultiScheme scheme;
    protected SpoutOutputCollector _collector;

    protected Queue<GroupedEvents> toEmitEvents = Lists.newLinkedList();
    protected Map<String, GroupedEvents> inProcessEvents = Maps.newLinkedHashMap();

    protected UnSideliningConfig config;


    public UnsideliningSpout(String topologyName, Provider<StormUnsideliner> sidelineProvider, MultiScheme scheme, String unsidelineStream, UnSideliningConfig config) {
        this.topologyName = topologyName;
        this.sidelineProvider = sidelineProvider;
        this.scheme = scheme;
        this.unsidelineStream = unsidelineStream;
        this.config = config;
    }

}
