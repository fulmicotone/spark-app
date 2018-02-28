package com.fulmicotone.spark.java.app;

import com.fulmicotone.spark.java.app.function.time.LocalDateToPartitionedStringPath;
import com.fulmicotone.spark.java.app.function.time.LocalDateToPartitionedStringS3Path;

import java.time.LocalDateTime;

public class PathDecoder {

    private final static String PATH_FORMAT = "%s/%s";

    private final StepArg arg;

    public enum Direction {input, output}

    public PathDecoder(StepArg arg) {
        this.arg = arg;
    }

    public String getAsString(Direction backet, String key) {
        return String
                .format(PATH_FORMAT,
                        Direction.input == backet ?
                                arg.inputPath :
                                arg.outputPath, key);
    }

    public String getInputPath(LocalDateTime ldt, String key,int wildDeepLevel) {

        return getOnDate(ldt, key, Direction.input, false,wildDeepLevel);
    }

    public String getInputAWSPath(LocalDateTime ldt, String key,int wildDeepLevel) {
        return getOnDate(ldt,
                key,
                Direction.input,
                true,
                wildDeepLevel);
    }


    public String getOutputPath(LocalDateTime ldt, String key,int wildDeepLevel) {

        return getOnDate(ldt,
                key,
                Direction.output,
                false,
                wildDeepLevel);
    }

    public String getOutputAWSPath(LocalDateTime ldt, String key,int wildDeepLevel) {

        return getOnDate(ldt,
                key,
                Direction.output,
                true,
                wildDeepLevel);
    }

    private String getOnDate(LocalDateTime ldt,
                             String key,
                             Direction direction,
                             boolean asAwsStream,
                             int wildDeepLevel) {

        String path= getAsString(direction, key) + "/" + (asAwsStream ?
                new LocalDateToPartitionedStringS3Path().apply(ldt) :
                new LocalDateToPartitionedStringPath().apply(ldt));
        for(int i=0;i<wildDeepLevel;i++){path+="/*";}
        return path ;
    }

}
