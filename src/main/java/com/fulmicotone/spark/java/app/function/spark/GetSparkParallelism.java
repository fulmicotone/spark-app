package com.fulmicotone.spark.java.app.function.spark;

import org.apache.spark.sql.SparkSession;
import scala.Option;

import java.util.function.Function;

public class GetSparkParallelism implements Function<SparkSession,Integer> {


    @Override
    public Integer apply(SparkSession session) {
        int defaultParallelism=2;
        Option<String> opt;
        if((opt=session.conf().getOption("spark.default.parallelism")).isEmpty()==false) {
            defaultParallelism = Integer.parseInt(opt.get());
        }
        return defaultParallelism;
    }
}
