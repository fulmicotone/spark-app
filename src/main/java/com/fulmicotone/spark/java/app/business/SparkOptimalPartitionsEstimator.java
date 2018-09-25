package com.fulmicotone.spark.java.app.business;

import  static com.fulmicotone.spark.java.app.utils.SparkConstantsProperties.*;
import org.apache.spark.SparkContext;


public  class SparkOptimalPartitionsEstimator extends OptimalPartitionsEstimator {


    private final SparkContext c;

    /**
     *
     *minPartitions example of coalesce 1 number of partitions in a single core (number of core per node)
     * @ maxPartitions numbers of core * number of executors
     */
    public SparkOptimalPartitionsEstimator(SparkContext c) {

        super(c.conf().getInt(spark_executor_cores,2),
               c.conf().getInt(spark_executor_cores,2)*
                c.conf().getInt(spark_executor_instances, c.getExecutorStorageStatus().length - 1));
        this.c=c;
    }

    public  int estimatePartition( Object datasetObject, long collectionSize ){

        return    estimatePartition(datasetObject, collectionSize,c.conf()
                .getSizeAsBytes(spark_executor_memory));
    }


    public  int estimatePartition( long datasetObjectSize, long collectionSize ){

        return    estimatePartition(datasetObjectSize, collectionSize,c.conf()
                .getSizeAsBytes(spark_executor_memory));
    }




}

