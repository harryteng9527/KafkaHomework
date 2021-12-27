package org.example;

public class SplitArgs {
    private final String topic;
    private final int records;
    private final int recordSize;

    public SplitArgs(String topic, int records, int recordSize){
        this.topic=topic;
        this.records=records;
        this.recordSize=recordSize;
    }

}
