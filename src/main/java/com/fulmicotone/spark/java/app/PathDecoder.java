package com.fulmicotone.spark.java.app;

import com.fulmicotone.spark.java.app.function.path.WildOnPath;
import com.fulmicotone.spark.java.app.function.time.LocalDateToPartitionedStringPath;
import com.fulmicotone.spark.java.app.function.time.LocalDateToPartitionedStringS3Path;
import com.fulmicotone.spark.java.app.function.time.LocalDateToPeriodPartitionedStringPath;

import java.io.Serializable;
import java.time.LocalDateTime;
import java.util.List;
import java.util.stream.Collectors;

public class PathDecoder implements Serializable {

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


    public List<String> getInputPathsByPeriod(LocalDateTime ldt, String key, int days,int wildDeepLevel){


        WildOnPath fn = new WildOnPath();
        final String baseBacketPath = getAsString(Direction.input, key) + "/";

       return  new LocalDateToPeriodPartitionedStringPath().apply(ldt,days).stream()

                .map(periodPath->baseBacketPath+periodPath).map(path->fn.apply(path,wildDeepLevel))
               .collect(Collectors.toList());



    }

    private String getOnDate(LocalDateTime ldt,
                             String key,
                             Direction direction,
                             boolean asAwsStream,
                             int wildDeepLevel) {

        String path= getAsString(direction, key) + "/" + (asAwsStream ?
                new LocalDateToPartitionedStringS3Path().apply(ldt) :
                new LocalDateToPartitionedStringPath().apply(ldt));

       return new WildOnPath().apply(path,wildDeepLevel);
    }

}
