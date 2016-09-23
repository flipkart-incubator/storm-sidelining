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

    public void insert(HBaseClient client, Message message) throws HBaseClientException{
        String row = message.getTopic() + message.getGroupId();
        client.putColumn(HBaseTableConfig.TABLE_NAME, row, HBaseTableConfig.COL_FAMILY_ATTRIBUTES, HBaseTableConfig.ATTR_TOPIC, Bytes.toBytes(message.getTopic()));
        client.putColumn(HBaseTableConfig.TABLE_NAME, row, HBaseTableConfig.COL_FAMILY_ATTRIBUTES, HBaseTableConfig.ATTR_GROUPID, Bytes.toBytes(message.getGroupId()));
        client.putColumn(HBaseTableConfig.TABLE_NAME, row, HBaseTableConfig.COL_FAMILY_DATA, message.getId(), message.getData());
    }

    public void insert(HBaseClient client, String topic, String groupId, Map<String, byte[]> map) throws HBaseClientException {
        String row = topic + groupId;
        client.putColumn(HBaseTableConfig.TABLE_NAME, row, HBaseTableConfig.COL_FAMILY_ATTRIBUTES, HBaseTableConfig.ATTR_TOPIC, Bytes.toBytes(topic));
        client.putColumn(HBaseTableConfig.TABLE_NAME, row, HBaseTableConfig.COL_FAMILY_ATTRIBUTES, HBaseTableConfig.ATTR_GROUPID, Bytes.toBytes(groupId));
        client.putColumns(HBaseTableConfig.TABLE_NAME, row, HBaseTableConfig.COL_FAMILY_DATA, map);
    }

    public void deleteRow(HBaseClient client, String topic, String groupId) throws HBaseClientException {
        String row = topic + groupId;
        client.clearRow(HBaseTableConfig.TABLE_NAME, row);
    }

    public Result get(HBaseClient client, String topic, String groupId) throws HBaseClientException {
        return client.getRow(HBaseTableConfig.TABLE_NAME, topic + groupId);
    }

    public void update( HBaseClient client, Message message) throws HBaseClientException {
        String row = message.getTopic() + message.getGroupId();
        client.updateColumn(HBaseTableConfig.TABLE_NAME, row, HBaseTableConfig.COL_FAMILY_DATA , message.getId(), message.getData());
    }

    public void deleteColumns(HBaseClient client, String topic, String groupId, List<String> ids) throws HBaseClientException {
        String row = topic + groupId;
        client.deleteColumns(HBaseTableConfig.TABLE_NAME, row, HBaseTableConfig.COL_FAMILY_DATA, ids);
    }

    public List<Result> search(HBaseClient client, String prefix) throws HBaseClientException {
        return client.scanPrefix(HBaseTableConfig.TABLE_NAME, prefix);
    }
}
