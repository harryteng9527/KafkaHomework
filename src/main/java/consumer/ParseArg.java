package consumer;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.JCommander;

public class ParseArg {
    @Parameter(
            names = "--bootstrapServer",
            description = "Specify the broker",
            required = true
    )
    public String bootstrapServer;

    @Parameter(
            names = "--strategy",
            description = "Specify the assignment strategy",
            required = false
    )
    public String assignmentStrategy;

    @Parameter(
            names = "--clientID",
            description = "An id string to pass to the server when making requests.",
            required = true
    )
    public String clientID;

    @Parameter(
            names = "--groupID",
            description = "A unique string that identifies the consumer group this consumer belongs to.",
            required = true
    )
    public String groupID;

    @Parameter(
            names = "--topics",
            description = "Subscribe topics to consume message",
            required = true
    )
    public String topics;


    public static ParseArg parseArg(String[] args){
        ParseArg argument = new ParseArg();
        JCommander.newBuilder()
                .addObject(argument)
                .build()
                .parse(args);

        return argument;
    }
}
