package com.fulmicotone.spark.java.app.business;

import org.apache.spark.util.SizeEstimator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;


public class OptimalPartitionsEstimator implements Serializable {


    private static Logger log= LoggerFactory.getLogger(OptimalPartitionsEstimator.class);

    //memory allocation
    private final static float executor_memory_safety_percentage=0.90f;
    //public final static float executor_memory_shuffle_percentage=0.20f;
    private final static float executor_memory_storage_percentage=0.60f;
    private final  int maxPartitions;
    private final  int minPartitions;
    /**
     *
     * @param minPartitions example of coalesce 1 number of partitions in a single core (number of core per node)
     * @param maxPartitions numbers of core * number of executors
     */
    public OptimalPartitionsEstimator(int minPartitions,int maxPartitions){

        this.minPartitions=minPartitions;
        this.maxPartitions=maxPartitions;


    }


    public  int estimatePartition(Object datasetObject,
                                           long collectionSize,
                                           long executorMemoryBytes){

        long singleExecutorMemoryBytes = (long) (executorMemoryBytes *
                        executor_memory_safety_percentage*
                        executor_memory_storage_percentage);


        log.info("collections size:"+collectionSize);

        log.info("min partition: {} - max partition {}",minPartitions,maxPartitions);

        log.info("singleExecutorMemoryBytes:"+singleExecutorMemoryBytes+" bytes");

        long singleObjectSize = SizeEstimator.estimate(datasetObject);

        log.info("single record:"+singleObjectSize+" bytes");

        long collectionSizeInBytes=singleObjectSize*collectionSize;

        log.info("collectionSizeInBytes:"+collectionSizeInBytes+" bytes");

        int partitions = Math.min(maxPartitions,
                (int) Math
                        .max((collectionSizeInBytes < singleExecutorMemoryBytes ? 1f :
                                (collectionSizeInBytes / singleExecutorMemoryBytes)), minPartitions));


        log.info("optimal partitioning with: "+partitions+" partitions");

        return partitions;

    }

}

