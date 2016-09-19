package com.flipkart.message.sidelining.dao;

import com.flipkart.message.sidelining.client.HBaseClientException;
import com.flipkart.message.sidelining.models.Message;

import java.util.Map;

/**
 * Created by saurabh.jha on 19/09/16.
 */
public class HBaseDAO {

    public void insert(Message message) throws HBaseClientException{

    }

    public void insert(String topic, String groupId, Map<String, byte[]> map) throws HBaseClientException {

    }
}
