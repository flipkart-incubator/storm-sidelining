package com.flipkart.message.sidelining.dao;

import com.flipkart.message.sidelining.client.HBaseClient;
import com.flipkart.message.sidelining.client.HBaseClientException;
import com.flipkart.message.sidelining.configs.HBaseTableConfig;
import com.flipkart.message.sidelining.models.Message;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.List;
import java.util.Map;

/**
 * Created by saurabh.jha on 19/09/16.
 */
public class HBaseDAO {

    public void insert(HBaseClient client, Message message, String tableName) throws HBaseClientException{
        String row = message.getTopic() + message.getGroupId();
        client.putColumn(tableName, row, HBaseTableConfig.COL_FAMILY_ATTRIBUTES, HBaseTableConfig.ATTR_TOPIC, Bytes.toBytes(message.getTopic()));
        client.putColumn(tableName, row, HBaseTableConfig.COL_FAMILY_ATTRIBUTES, HBaseTableConfig.ATTR_GROUPID, Bytes.toBytes(message.getGroupId()));
        client.putColumn(tableName, row, HBaseTableConfig.COL_FAMILY_DATA, message.getId(), message.getData());
    }

    public void insert(HBaseClient client, String topic, String groupId, Map<String, byte[]> map, String tableName) throws HBaseClientException {
        String row = topic + groupId;
        client.putColumn(tableName, row, HBaseTableConfig.COL_FAMILY_ATTRIBUTES, HBaseTableConfig.ATTR_TOPIC, Bytes.toBytes(topic));
        client.putColumn(tableName, row, HBaseTableConfig.COL_FAMILY_ATTRIBUTES, HBaseTableConfig.ATTR_GROUPID, Bytes.toBytes(groupId));
        client.putColumns(tableName, row, HBaseTableConfig.COL_FAMILY_DATA, map);
    }

    public void deleteRow(HBaseClient client, String topic, String groupId, String tableName) throws HBaseClientException {
        String row = topic + groupId;
        client.clearRow(tableName, row);
    }

    public Result get(HBaseClient client, String topic, String groupId, String tableName) throws HBaseClientException {
        return client.getRow(tableName, topic + groupId);
    }

    public void update( HBaseClient client, Message message, String tableName) throws HBaseClientException {
        String row = message.getTopic() + message.getGroupId();
        client.updateColumn(tableName, row, HBaseTableConfig.COL_FAMILY_DATA , message.getId(), message.getData());
    }

    public void deleteColumns(HBaseClient client, String topic, String groupId, List<String> ids, String tableName) throws HBaseClientException {
        String row = topic + groupId;
        client.deleteColumns(tableName, row, HBaseTableConfig.COL_FAMILY_DATA, ids);
    }

    public List<Result> search(HBaseClient client, String prefix, String tableName) throws HBaseClientException {
        return client.scanPrefix(tableName, prefix);
    }
}
