package com.flipkart.storm.sidelining.core.spout;

import com.flipkart.storm.sidelining.core.exceptions.SideliningException;
import com.flipkart.storm.sidelining.core.spout.model.GroupedEvents;

import java.util.List;

/**
 * Created by gupta.rajat on 27/07/17.
 */
public interface StormUnsideliner {

    List<GroupedEvents> getGroupedEvents(List<String> rows) throws SideliningException;

    List<GroupedEvents> getGroupedEvents(String firstRow, String topology, int batch) throws SideliningException;

    List<String> getUnsidelinedRowKeys(String firstRow, String topology, int batch) throws SideliningException;

    public void clearUnsidelinedKeys(List<String> rowKeys) ;

    public boolean checkAndDeleteRow(String topic, String groupId, long version);

    public boolean checkAndDeleteRow(String rowKey, long version);

    public boolean deleteEvent(String rowKey, String column);

    public boolean deleteEvent(String topic, String groupId, String column);

}
