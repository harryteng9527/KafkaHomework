package org.example;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.JCommander;

/**
 * This class is data block for storing the data which pass into the producer application.
 */
public class Argument {

    @Parameter(names = "--brokers")
    public String bootstrapServer;

    @Parameter( names = "--topic")
    public String topicName;

    @Parameter( names = "--records")
    public int records;

    @Parameter(names = "--recordSize")
    public int recordSize;

    Argument(){}

    public static Argument parse(String[] args){
        Argument arg = new Argument();
        JCommander.newBuilder()
                .addObject(arg)
                .build()
                .parse(args);

        return arg;
    }
}
