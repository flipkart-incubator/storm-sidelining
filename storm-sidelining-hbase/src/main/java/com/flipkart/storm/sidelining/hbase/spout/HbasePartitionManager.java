package com.flipkart.storm.sidelining.hbase.spout;

import com.flipkart.storm.sidelining.core.spout.PartitionManager;
import com.flipkart.storm.sidelining.core.spout.StormUnsideliner;
import com.flipkart.storm.sidelining.core.spout.UnSideliningConfig;
import com.flipkart.storm.sidelining.core.spout.model.GroupedEvents;
import com.flipkart.storm.sidelining.core.spout.model.PartitionState;
import com.flipkart.storm.sidelining.hbase.unsideline.HbaseStormUnsideliner;
import com.google.common.collect.Lists;

import java.util.List;
import java.util.Queue;

public class HbasePartitionManager implements PartitionManager{

    private int partition;
    private String firstRow;
    private String TOPOLOGY_PREFIX;

    private StormUnsideliner stormUnsideliner;
    private String topologyName;

    private UnSideliningConfig config;


    public HbasePartitionManager(StormUnsideliner stormUnsideliner, int partition, String topologyName, UnSideliningConfig config) {
        this.stormUnsideliner = stormUnsideliner;
        this.partition = partition;
        TOPOLOGY_PREFIX = partition + "-" + topologyName;
        this.firstRow = TOPOLOGY_PREFIX;
        this.topologyName = topologyName;
        this.config = config;
    }

    public PartitionState fillEmitQueue(Queue<GroupedEvents> toEmitEvents) {
        try {

            List<GroupedEvents> toEmitGroups = Lists.newArrayList();

            if (config.mode == UnSideliningConfig.Mode.SEMI_AUTOMATIC) {
                List<String> rows = stormUnsideliner.getUnsidelinedRowKeys(firstRow, partition + "-" + topologyName, config.batch);
                stormUnsideliner.clearUnsidelinedKeys(rows);
                toEmitGroups = stormUnsideliner.getGroupedEvents(rows);
            } else if (config.mode == UnSideliningConfig.Mode.AUTOMATIC) {
                toEmitGroups = stormUnsideliner.getGroupedEvents(firstRow, partition + "-" + topologyName, config.batch);
            }

            toEmitEvents.addAll(toEmitGroups);

            if (toEmitGroups.size() == config.batch) {
                firstRow = toEmitGroups.get(toEmitGroups.size() - 1).rowKey;
                return PartitionState.SCAN_PENDING;
            } else {
                firstRow = TOPOLOGY_PREFIX;
                return PartitionState.SCAN_DONE;
            }
        } catch (Exception e) {
            throw new RuntimeException("Hbase unsideline scan failed", e);
        }
    }

}