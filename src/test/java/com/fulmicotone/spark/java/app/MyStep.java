package com.fulmicotone.spark.java.app;

import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;

import java.util.Arrays;

public class MyStep extends Step {

    @Override
    protected void beforeRun(StepArg arg, SparkSession sparkSession) {



        wrapDataset(   sparkSession
                .createDataset(Arrays.asList("BEFORERUN"), Encoders.STRING()));

    }

    @Override
    protected void run(StepArg arg, SparkSession sparkSession) {



       wrapDataset(unwrapDataset().orElse(sparkSession.emptyDataset(Encoders.STRING())).union(sparkSession
               .createDataset(Arrays.asList("RUN"), Encoders.STRING())));


    }


    @Override
    protected void afterRun(StepArg arg, SparkSession sparkSession) {
        wrapDataset(unwrapDataset().get().union(sparkSession
                .createDataset(Arrays.asList("AFTERRUN"), Encoders.STRING())));
    }
}
