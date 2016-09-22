package com.flipkart.message.sidelining.configs;

import lombok.Data;

/**
 * Created by saurabh.jha on 18/09/16.
 */
@Data
public class HBaseClientConfig {
    public static final String zookeeperQuorum = "127.0.0.1";
    public static final Integer port = 60000;
    public static final int poolSize = 5;
}
