package com.flipkart.message.sidelining.configs;

import lombok.Data;

/**
 * Created by saurabh.jha on 18/09/16.
 */
@Data
public class HBaseClientConfig {
    public static final String zookeeperQuorum = "";
    public static final Integer port = 3000;
    public static final int poolSize = 10;
}
