package com.flipkart.storm.sidelining.hbase.unsideline;

import com.flipkart.storm.sidelining.core.spout.StormUnsideliner;
import com.flipkart.storm.sidelining.core.spout.model.GroupedEvents;
import com.flipkart.storm.sidelining.hbase.client.HBaseClient;
import com.flipkart.storm.sidelining.hbase.client.HBaseClientException;
import com.flipkart.storm.sidelining.hbase.dao.HbaseHandler;
import com.google.common.collect.Lists;
import org.apache.hadoop.hbase.client.HTablePool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Created by gupta.rajat on 27/07/17.
 */
public class HbaseStormUnsideliner implements StormUnsideliner {

    private static final Logger log = LoggerFactory.getLogger(HbaseStormUnsideliner.class);

    private HbaseHandler hbaseHandler;

    public HbaseStormUnsideliner(HTablePool tablePool, String tableName, String unsidelineTable) {
        HBaseClient client = new HBaseClient(tablePool);
        hbaseHandler = new HbaseHandler(client, tableName, unsidelineTable);
    }

    public List<GroupedEvents> getGroupedEvents(List<String> rows) throws HBaseClientException {
        return hbaseHandler.getGroupedEvents(rows);
    }

    public List<GroupedEvents> getGroupedEvents(String firstRow, String topology, int batch) throws HBaseClientException {
        return hbaseHandler.getGroupedEvents(firstRow, topology, batch);
    }

    public List<String> getUnsidelinedRowKeys(String firstRow, String topology, int batch) throws HBaseClientException {
        return hbaseHandler.getUnsidelinedRowKeys(firstRow, topology, batch);
    }

    public void clearUnsidelinedKeys(List<String> rowKeys) {
        try {
            hbaseHandler.deleteUnsidelinedRows(rowKeys);
        } catch (HBaseClientException e) {
            log.error("Error deleting rowKeys {}", rowKeys, e);
        }
    }

    public boolean checkAndDeleteRow(String topic, String groupId, long version) {
        log.info("deleting data for topic {} and groupId {}", topic, groupId);
        try {
            hbaseHandler.checkAndDeleteRow(topic, groupId, version);
            return true;
        } catch (HBaseClientException e) {
            log.error("error while replaying the data {}", e);
            return false;
        }

    }

    public boolean checkAndDeleteRow(String rowKey, long version) {
        log.info("deleting data for rowKey {}", rowKey);
        try {
            hbaseHandler.checkAndDeleteRow(rowKey, version);
            return true;
        } catch (HBaseClientException e) {
            log.error("error while replaying the data {}", e);
            return false;
        }
    }


    public boolean deleteEvent(String topic, String groupId, String column) {
        log.info("deleting data for topic {}, groupId {} and column {}", topic, groupId, column);
        try {
            hbaseHandler.deleteColumns(topic, groupId, Lists.newArrayList(column));
            return true;
        } catch (HBaseClientException e) {
            log.error("error while deleting the data {}", e);
            return false;
        }
    }

    public boolean deleteEvent(String rowKey, String column) {
        log.info("deleting data for rowKey {} and column {}", rowKey, column);
        try {
            hbaseHandler.deleteColumn(rowKey, column);
            return true;
        } catch (HBaseClientException e) {
            log.error("error while deleting the data {}", e);
            return false;
        }
    }

}
