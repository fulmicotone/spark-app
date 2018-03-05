package com.fulmicotone.spark.java.app.processor;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.time.LocalDateTime;


public interface ITimeDatasetProcessor {


     Dataset apply(Dataset dsupp, LocalDateTime scheduledTime, LocalDateTime executionTime);
}