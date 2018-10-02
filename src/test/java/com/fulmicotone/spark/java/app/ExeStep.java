package com.fulmicotone.spark.java.app;

import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;

import java.util.Arrays;

public class ExeStep extends Step {

    @Override
    protected void beforeRun(StepArg arg, SparkSession sparkSession) {




    }

    @Override
    protected void run(StepArg arg, SparkSession sparkSession) {



        sparkSession.read().csv("/Users/dino/Desktop/FL_insurance_sample.csv").show();


    }


    @Override
    protected void afterRun(StepArg arg, SparkSession sparkSession) {
    }
}
