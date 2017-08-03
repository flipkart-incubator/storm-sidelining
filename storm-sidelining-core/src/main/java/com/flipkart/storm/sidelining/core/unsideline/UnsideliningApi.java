package com.flipkart.storm.sidelining.core.unsideline;

import com.flipkart.storm.sidelining.core.exceptions.SideliningException;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Created by gupta.rajat on 27/07/17.
 */

/*
 * Used for semi_auto unsidelining
 */
public interface UnsideliningApi {
    public void unsidelineAll(int batch) throws SideliningException ;
    public void unsidelineTopic(String topic, int batch) throws SideliningException;
    public void unsidelineGroup(String rowKey) throws SideliningException;
    public List<String> getSidelinedRowKeys(int batch) throws SideliningException;
    public List<String> getSidelinedRowKeys(String topology, int batch) throws SideliningException;
    public Map<String, String> getAllEvents(String rowKey) throws SideliningException;
    public Map<String, String> getEvent(String rowKey, String eventId) throws SideliningException;
    public Map<String, String> getEvent(String topic, String groupId, String eventId) throws SideliningException;
    public boolean deleteRow(String topic, String groupId);
    public boolean deleteEvent(String topic, String groupId, String column);
    public boolean deleteEvent(String rowKey, String column);
    public boolean update(String topic, String groupId, String id, ModifyFunc modifier) throws SideliningException, IOException;
}