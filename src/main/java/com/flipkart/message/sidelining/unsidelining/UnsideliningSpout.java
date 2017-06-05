package com.flipkart.message.sidelining.unsidelining;

import backtype.storm.spout.MultiScheme;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.flipkart.message.sidelining.configs.HBaseTableConfig;
import com.flipkart.message.sidelining.service.SidelineService;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.inject.Provider;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;


/**
 * Created by gupta.rajat on 04/01/17.
 */
public class UnsideliningSpout extends BaseRichSpout {
    private static final Logger log = LoggerFactory.getLogger(UnsideliningSpout.class);

    private String unsidelineStream;

    private String topologyName;

    private List<HbasePartitionManager> partitionManagers = Lists.newArrayList();
    private int currentPartitionIndex = 0;
    private PartitionState currentPartitionState = PartitionState.SCAN_DONE;

    private final Provider<SidelineService> sidelineProvider;
    private SidelineService sideliner;
    private Map config;
    private MultiScheme scheme;
    private SpoutOutputCollector _collector;

    private Queue<GroupedEvents> toEmitEvents = Lists.newLinkedList();
    private Map<String, GroupedEvents> inProcessEvents = Maps.newLinkedHashMap();

    private int batchSize = 10;

    public UnsideliningSpout(String topologyName, Provider<SidelineService> sidelineProvider, MultiScheme scheme, String unsidelineStream, int batchSize) {
        this.topologyName = topologyName;
        this.sidelineProvider = sidelineProvider;
        this.scheme = scheme;
        this.batchSize = batchSize;
        this.unsidelineStream = unsidelineStream;
    }

    /*
     * Called when a task for this component is initialized within a worker on the cluster.
     * It provides the spout with the environment in which the spout executes.
     */
    @Override
    public void open(Map config, TopologyContext context, SpoutOutputCollector collector) {
        this._collector = collector;
        this.config = config;

        sideliner = sidelineProvider.get();

        int totalPartions = 512;
        int taskId = context.getThisTaskId();
        String componentId = context.getComponentId(taskId);
        int totalSpouts = context.getComponentTasks(componentId).size();
        int rem = taskId % totalSpouts;

        for (int partition = 0; partition < totalPartions; partition++) {
            if (partition % totalSpouts == rem) {
                log.info("Partition no. {} attached to taskId {}", partition, taskId);
                partitionManagers.add(new HbasePartitionManager(partition));
            }
        }
        log.info("UnSpout details" + context.toJSONString());
        log.info("Topology streams " + context.getThisStreams().toString());
        log.info("UnSpout targets" + context.getThisTargets().toString());
    }

    /*
     * When this method is called, Storm is requesting that the Spout emit tuples to the output collector.
     * This method should be non-blocking, so if the Spout has no tuples to emit, this method should return.
     * nextTuple, ack, and fail are all called in a tight loop in a single thread in the spout task.
     * When there are no tuples to emit, it is courteous to have nextTuple sleep for a short amount of time
     * (like a single millisecond) so as not to waste too much CPU.
     */
    @Override
    public void nextTuple() {
        sanityCheck();

        try {
            if (toEmitEvents.isEmpty()) {
                if (currentPartitionState == PartitionState.SCAN_DONE) {
                    currentPartitionIndex = (currentPartitionIndex + 1) % partitionManagers.size();
                    if (currentPartitionIndex == partitionManagers.size() - 1) {
                        try {
                            log.info("Sleeping for 1min as all scans done.");
                            Thread.sleep(60000L);
                        } catch (InterruptedException ignored) {

                        }
                    }
                }
                currentPartitionState = partitionManagers.get(currentPartitionIndex).fillEmitQueue();
            } else {
                GroupedEvents groupedEvents = toEmitEvents.remove();
                Event event = groupedEvents.eventQueue.peek();
                List<Object> tuple = generateTuple(event.value);
                StormMsgId stormMsgId = new StormMsgId(groupedEvents.rowKey, event.id);

                log.info("Emitting tuple rowKey {}, id {} to stream {}", stormMsgId.rowKey, stormMsgId.messageId, unsidelineStream);
                _collector.emit(unsidelineStream, tuple, stormMsgId);

                inProcessEvents.put(groupedEvents.rowKey, groupedEvents);
            }
        } catch (Exception e) {
            log.error("nextTuple failed ",e);
        }
    }

    /**
     * //TODO Use same serializer and deserializer.
     * //Maintains ordering
     */
    private List<Object> generateTuple(byte[] value) {
        try {
            LinkedHashMap<String, Object> result = new ObjectMapper().readValue(value, LinkedHashMap.class);
            List<Object> ret = new ArrayList<>(result.size());
            for (String s : scheme.getOutputFields()) {
                ret.add(result.get(s));
            }
            return ret;
        } catch (IOException e) {
            log.error("Deserialization error", e);
        }
        return null;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream(unsidelineStream, scheme.getOutputFields());
    }

    /**
     * Storm has determined that the tuple emitted by this spout with the msgId identifier has been fully processed.
     * Typically, an implementation of this method will take that message off the queue and prevent it from being replayed.
     *
     * @param msgId
     */

    @Override
    public void ack(Object msgId) {
        StormMsgId stormMsgId = (StormMsgId) msgId;

        log.info("Got ack for {}, {}",stormMsgId.rowKey,stormMsgId.messageId);

        GroupedEvents groupedEvents = inProcessEvents.get(stormMsgId.rowKey);
        if(groupedEvents!=null) {
            Event event = groupedEvents.eventQueue.peek();
            if (event.id.equals(stormMsgId.messageId)) { // there is no exactly once guarantee
                groupedEvents.eventQueue.remove();
                if (!groupedEvents.eventQueue.isEmpty()) {
                    log.info("Adding this group to emitQueue {}", groupedEvents.rowKey);
                    sideliner.deleteEvent(stormMsgId.rowKey, stormMsgId.messageId);
                    toEmitEvents.add(groupedEvents);
                } else {
                    log.info("Deleting this group as no events in group {}", groupedEvents.rowKey);
                    sideliner.checkAndDeleteRow(groupedEvents.rowKey, groupedEvents.version);
                }
                inProcessEvents.remove(groupedEvents.rowKey);
            }
        }
    }

    @Override
    public void fail(Object msgId) {
        StormMsgId stormMsgId = (StormMsgId) msgId;
        log.info("Removing from map , Got fail for {}, {}",stormMsgId.rowKey,stormMsgId.messageId);
        inProcessEvents.remove(stormMsgId.rowKey);
    }

    private void sanityCheck() {
        if (inProcessEvents.size() > 10000) {
            log.error("Events not getting ack or fail");
            inProcessEvents.clear();
        }
    }

    /**
     * Called when an ISpout is going to be shutdown.
     * There is no guarantee that close will be called, because the supervisor kill -9's worker processes on the cluster.
     * The one context where close is guaranteed to be called is a topology is killed when running Storm in local mode.
     */
    @Override
    public void close() {

    }

    private enum PartitionState {
        SCAN_DONE,
        SCAN_PENDING
    }

    public static class StormMsgId {
        public String rowKey;
        public String messageId;

        public StormMsgId(String rowKey, String messageId) {
            this.rowKey = rowKey;
            this.messageId = messageId;
        }
    }

    private class GroupedEvents {
        String rowKey;
        Queue<Event> eventQueue = Lists.newLinkedList();
        long version;
    }

    private class Event {
        String id;
        byte[] value;

        private Event(String id, byte[] value) {
            this.id = id;
            this.value = value;
        }
    }

    private class HbasePartitionManager {

        int partition;
        String TOPOLOGY_PREFIX ;
        String firstRow ;

        private HbasePartitionManager(int partition) {
            this.partition = partition;
            TOPOLOGY_PREFIX = partition + "-" + topologyName;
            this.firstRow = TOPOLOGY_PREFIX;
        }

        private PartitionState fillEmitQueue() {
            try {

                ArrayList<Result> resultList = sideliner.scan(firstRow, partition + "-" + topologyName, batchSize);

                List<GroupedEvents> toEmitGroups = transform(resultList);
                toEmitEvents.addAll(toEmitGroups);

                if (resultList.size() == batchSize) {
                    firstRow = Bytes.toString(resultList.get(resultList.size() - 1).getRow());
                    return PartitionState.SCAN_PENDING;
                } else {
                    firstRow = TOPOLOGY_PREFIX;
                    return PartitionState.SCAN_DONE;
                }
            } catch (Exception e) {
                throw new RuntimeException("Hbase unsideline scan failed", e);
            }
        }

        private List<GroupedEvents> transform(ArrayList<Result> resultList) {


            List<GroupedEvents> toEmitGroups = Lists.newLinkedList();
            for (Result result : resultList) {
                log.info("Transforming result for {}",result.getRow());
                List<KeyValue> list = result.list();
                list.sort((o1, o2) -> (int) (o1.getTimestamp() - o2.getTimestamp()));
                GroupedEvents groupedEvents = new GroupedEvents();
                groupedEvents.rowKey = Bytes.toString(result.getRow());
                for (KeyValue kv : list) {
                    if (Bytes.toString(kv.getQualifier()).equals(HBaseTableConfig.VERSION)) {
                        groupedEvents.version = Bytes.toLong(kv.getValue());
                    } else {
                        groupedEvents.eventQueue.add(new Event(Bytes.toString(kv.getQualifier()), kv.getValue()));
                    }
                }
                toEmitGroups.add(groupedEvents);
            }
            return toEmitGroups;
        }

    }

}
