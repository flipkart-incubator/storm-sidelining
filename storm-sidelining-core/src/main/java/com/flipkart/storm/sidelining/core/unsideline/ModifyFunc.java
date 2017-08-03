package com.flipkart.storm.sidelining.core.unsideline;

import java.util.Map;

/**
 * Created by gupta.rajat on 14/06/17.
 */
@FunctionalInterface
public interface ModifyFunc {
    Map<String, Object> modify(Map<String, Object> tuple);
}
