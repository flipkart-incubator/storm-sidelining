package com.flipkart.storm.sidelining.hbase.spout;

import backtype.storm.spout.MultiScheme;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import com.fasterxml.jackson.core.type.TypeReference;
import com.flipkart.storm.sidelining.core.spout.StormUnsideliner;
import com.flipkart.storm.sidelining.core.spout.UnSideliningConfig;
import com.flipkart.storm.sidelining.core.spout.UnsideliningSpout;
import com.flipkart.storm.sidelining.core.spout.model.Event;
import com.flipkart.storm.sidelining.core.spout.model.GroupedEvents;
import com.flipkart.storm.sidelining.core.spout.model.PartitionState;
import com.flipkart.storm.sidelining.core.spout.model.StormMsgId;
import com.flipkart.storm.sidelining.core.utils.SerdeUtils;
import com.flipkart.storm.sidelining.hbase.unsideline.HbaseStormUnsideliner;
import com.google.inject.Provider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;


/**
 * Created by gupta.rajat on 04/01/17.
 */
public class HbaseUnsideliningSpout extends UnsideliningSpout {
    private static final Logger log = LoggerFactory.getLogger(HbaseUnsideliningSpout.class);

    public HbaseUnsideliningSpout(String topologyName, Provider<StormUnsideliner> sidelineProvider, MultiScheme scheme, String unsidelineStream, UnSideliningConfig config) {
        super(topologyName, sidelineProvider, scheme, unsidelineStream, config);
    }

    /*
     * Called when a task for this component is initialized within a worker on the cluster.
     * It provides the spout with the environment in which the spout executes.
     */
    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this._collector = collector;

        stormUnsideliner = sidelineProvider.get();

        int taskId = context.getThisTaskId();
        String componentId = context.getComponentId(taskId);
        int totalSpouts = context.getComponentTasks(componentId).size();
        int rem = taskId % totalSpouts;

        for (int partition = 0; partition < config.partitions; partition++) {
            if (partition % totalSpouts == rem) {
                log.info("Partition no. {} attached to taskId {}", partition, taskId);
                partitionManagers.add(new HbasePartitionManager(stormUnsideliner, partition, topologyName, config));
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
                currentPartitionState = partitionManagers.get(currentPartitionIndex).fillEmitQueue(toEmitEvents);
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
            log.error("nextTuple failed ", e);
        }
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

        log.info("Got ack for {}, {}", stormMsgId.rowKey, stormMsgId.messageId);

        GroupedEvents groupedEvents = inProcessEvents.get(stormMsgId.rowKey);
        if (groupedEvents != null) {
            Event event = groupedEvents.eventQueue.peek();
            if (event.id.equals(stormMsgId.messageId)) { // there is no exactly once guarantee
                groupedEvents.eventQueue.remove();
                if (!groupedEvents.eventQueue.isEmpty()) {
                    log.info("Adding this group to emitQueue {}", groupedEvents.rowKey);
                    stormUnsideliner.deleteEvent(stormMsgId.rowKey, stormMsgId.messageId);
                    toEmitEvents.add(groupedEvents);
                } else {
                    log.info("Deleting this group as no events in group {}", groupedEvents.rowKey);
                    stormUnsideliner.checkAndDeleteRow(groupedEvents.rowKey, groupedEvents.version);
                }
                inProcessEvents.remove(groupedEvents.rowKey);
            }
        }
    }

    @Override
    public void fail(Object msgId) {
        StormMsgId stormMsgId = (StormMsgId) msgId;
        log.info("Removing from map , Got fail for {}, {}", stormMsgId.rowKey, stormMsgId.messageId);
        inProcessEvents.remove(stormMsgId.rowKey);
    }


    /**
     * Called when an ISpout is going to be shutdown.
     * There is no guarantee that close will be called, because the supervisor kill -9's worker processes on the cluster.
     * The one context where close is guaranteed to be called is a topology is killed when running Storm in local mode.
     */
    @Override
    public void close() {

    }

    /**
     * //Maintains ordering
     */
    private List<Object> generateTuple(byte[] value) {
        try {
            Map<String, Object> result = SerdeUtils.deserialize(value, new TypeReference<Map<String, Object>>() {
            });
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

    private void sanityCheck() {
        if (inProcessEvents.size() > 100) {
            log.error("Events not getting ack or fail");
            inProcessEvents.clear();
        }
    }

}
