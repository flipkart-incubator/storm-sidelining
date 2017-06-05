package com.flipkart.message.sidelining.models;

/**
 * Created by gupta.rajat on 05/06/17.
 */
public class Event {
    public String id;
    public byte[] value;

    public Event(String id, byte[] value) {
        this.id = id;
        this.value = value;
    }
}