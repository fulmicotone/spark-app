package com.fulmicotone.spark.java.app.processor.impl;

import com.fulmicotone.spark.java.app.processor.TimeDatasetProcessor;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.time.LocalDateTime;
import java.time.ZoneOffset;

import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.types.DataTypes.LongType;

public  class AddEtlCreationColumnProcessor extends TimeDatasetProcessor {


    @Override
    public Dataset apply(Dataset dsupp, LocalDateTime scheduledTime, LocalDateTime executionDate) {

        return dsupp.withColumn("etl_update_time",
                lit(scheduledTime.toInstant(ZoneOffset.UTC).toEpochMilli())
                .cast(LongType));
    }

}