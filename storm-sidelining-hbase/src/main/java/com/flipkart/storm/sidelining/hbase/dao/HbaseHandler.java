package com.flipkart.storm.sidelining.hbase.dao;

import com.flipkart.storm.sidelining.hbase.key.KeyFormatter;
import com.flipkart.storm.sidelining.hbase.client.HBaseClient;
import com.flipkart.storm.sidelining.hbase.client.HBaseClientException;
import com.flipkart.storm.sidelining.core.spout.model.Event;
import com.flipkart.storm.sidelining.core.spout.model.GroupedEvents;
import com.flipkart.storm.sidelining.core.sideline.models.Message;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Created by gupta.rajat on 05/06/17.
 */
public class HbaseHandler {

    //column family names
    public static final String CF = "cf";

    //attribute names
    public static final String VERSION = "_version";
    public static final String STATE = "state";

    private HBaseClient client;

    private static final KeyFormatter keyFormatter = new KeyFormatter.HbaseKeyFormatter();
    private String sidelineTable;
    private String unsidelineTable;

    public HbaseHandler(HBaseClient client, String sidelineTable, String unsidelineTable) {
        this.client = client;
        this.sidelineTable = sidelineTable;
        this.unsidelineTable = unsidelineTable;
    }

    public boolean insert(Message message) throws HBaseClientException {
        long oldVersion = getVersion(message.getTopic(), message.getGroupId());
        return checkAndPut(message,oldVersion);
    }

    public boolean checkAndPut(Message message, long oldVersion) throws HBaseClientException {
        byte[] checkVersionBytes = null;
        long newVersion = oldVersion + 1L;
        if(oldVersion == 0L) {
            checkVersionBytes = null;
        } else {
            checkVersionBytes = Bytes.toBytes(oldVersion);
        }
        Map<String, byte[]> cells = Maps.newHashMap();
        cells.put(message.getId(), message.getData());
        cells.put(VERSION, Bytes.toBytes(newVersion));
        return client.checkAndPutColumns(sidelineTable, keyFormatter.getRowKey(message.getTopic(),message.getGroupId()), CF, cells, VERSION, checkVersionBytes);
    }

    public List<GroupedEvents> getGroupedEvents(List<String> rowKeys) throws HBaseClientException {
         return transform(client.getRows(sidelineTable, rowKeys));
    }

    public List<GroupedEvents> getGroupedEvents(String firstRow, String topology, int batch) throws HBaseClientException {
        return transform(client.scanPrefix(sidelineTable,firstRow,topology,batch));
    }

    //API
    public void unsidelineAll(int batch) throws HBaseClientException {
        List<String> sidelinedRowKeys = getSidelinedRowKeys(batch);
        for(String sidelinedRowKey : sidelinedRowKeys) {
            unsideline(sidelinedRowKey);
        }
    }

    //API
    public void unsidelineTopic(String topic, int batch) throws HBaseClientException {
        List<String> sidelinedRowKeys = getSidelinedRowKeys(topic, batch);
        for(String sidelinedRowKey : sidelinedRowKeys) {
            unsideline(sidelinedRowKey);
        }
    }

    //API
    public void unsideline(String rowKey) throws HBaseClientException {
        if(!client.getRow(sidelineTable,rowKey).isEmpty()) {
            client.putColumn(unsidelineTable, rowKey, CF, STATE, "UNPROCESSED".getBytes());
        }
    }

    //API
    public Map<String, String> getAllEvents(String topic, String groupId) throws HBaseClientException {
        return getAllEvents(keyFormatter.getRowKey(topic, groupId));
    }

    //API
    public Map<String, String> getAllEvents(String rowKey) throws HBaseClientException {

        Map<String, String> events = Maps.newHashMap();
        Result result = client.getRow(sidelineTable, rowKey);
        NavigableMap<byte[], byte[]> familyMap = result.getFamilyMap(CF.getBytes());
        if(familyMap == null) {
            return null;
        }

        for (Map.Entry<byte[], byte[]> entry : familyMap.entrySet()) {
            events.put(Bytes.toString(entry.getKey()), Bytes.toString(entry.getValue()));
        }
        return events;
    }

    //API
    public Map<String, String> getEvent(String topic, String groupId, String eventId) throws HBaseClientException {
        Map<String, String> map = new HashMap<>();
        String rowKey = keyFormatter.getRowKey(topic, groupId);
        byte[] result = getEvent(rowKey,eventId);
        map.put(eventId, Bytes.toString(result));
        map.put("rowKey", rowKey);
        return map;
    }

    //API
    public byte[] getEvent(String rowKey, String eventId) throws HBaseClientException {
        return client.getColumnForRow(sidelineTable, rowKey, CF, eventId);
    }

    //API
    public List<String> getSidelinedRowKeys(int batch) throws HBaseClientException {
        return client.scanRowKeyOnly(sidelineTable,batch);
    }

    //API
    public List<String> getSidelinedRowKeys(String topology, int batch) throws HBaseClientException {
        return client.scanRowKeyOnly(sidelineTable,topology,batch);
    }

    public void deleteRow(String topic, String groupId) throws HBaseClientException {
        client.clearRow(sidelineTable, keyFormatter.getRowKey(topic, groupId));
    }
    public void deleteUnsidelinedRows(List<String> rowKeys) throws HBaseClientException {
        for(String rowKey : rowKeys) {
            client.clearRow(unsidelineTable, rowKey);
        }
    }

    public void checkAndDeleteRow(String topic, String groupId, long version) throws HBaseClientException {
        checkAndDeleteRow(keyFormatter.getRowKey(topic, groupId),version);
    }

    public void checkAndDeleteRow(String rowKey, long version) throws HBaseClientException {
        client.checkAndClearRow(sidelineTable, rowKey, CF, VERSION, version);
    }

    public void deleteColumns(String topic, String groupId, List<String> ids) throws HBaseClientException {
        client.deleteColumns(sidelineTable, keyFormatter.getRowKey(topic, groupId), CF, ids);
    }

    public void deleteColumn(String rowKey, String column) throws HBaseClientException {
        client.deleteColumns(sidelineTable, rowKey, CF, Lists.newArrayList(column));
    }

    public void update(Message message) throws HBaseClientException {
        client.updateColumn(sidelineTable, keyFormatter.getRowKey(message.getTopic(),message.getGroupId()), CF, message.getId(), message.getData());
    }

    public ArrayList<Result> getSidelinedRows(String firstRow, String prefix, int batch) throws HBaseClientException {
        return client.scanPrefix(sidelineTable, firstRow, prefix, batch);
    }

    public List<String> getUnsidelinedRowKeys(String firstRow, String prefix, int batch) throws HBaseClientException {
        List<Result> results =  client.scanPrefix(unsidelineTable, firstRow, prefix, batch);
        return results.stream().map(result -> Bytes.toString(result.getRow())).collect(Collectors.toList());
    }

    //API
    @Deprecated
    public int getTotalCount() throws HBaseClientException {
        return client.getCount(sidelineTable);
    }

    //API
    @Deprecated
    public int getTopicCount(String topic) throws HBaseClientException {
        return client.getCount(sidelineTable,topic);
    }

    public long getVersion(String topic, String groupId) throws HBaseClientException {
        byte[] version = client.getColumnForRow(sidelineTable, keyFormatter.getRowKey(topic, groupId), CF, VERSION);
        if(version == null) return 0;
        return Bytes.toLong(version);
    }


    private List<GroupedEvents> transform(List<Result> resultList) {

        List<GroupedEvents> toEmitGroups = Lists.newLinkedList();
        for (Result result : resultList) {
            List<KeyValue> list = result.list();
            if (list != null && list.size() != 0) {
                list.sort((o1, o2) -> (int) (o1.getTimestamp() - o2.getTimestamp()));
                GroupedEvents groupedEvents = new GroupedEvents();
                groupedEvents.rowKey = Bytes.toString(result.getRow());
                for (KeyValue kv : list) {
                    if (Bytes.toString(kv.getQualifier()).equals(VERSION)) {
                        groupedEvents.version = Bytes.toLong(kv.getValue());
                    } else {
                        groupedEvents.eventQueue.add(new Event(Bytes.toString(kv.getQualifier()), kv.getValue()));
                    }
                }
                toEmitGroups.add(groupedEvents);
            }
        }
        return toEmitGroups;
    }
}
