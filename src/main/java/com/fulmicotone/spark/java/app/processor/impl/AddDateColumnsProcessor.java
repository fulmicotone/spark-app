package com.fulmicotone.spark.java.app.processor.impl;

import com.fulmicotone.spark.java.app.function.time.TimeColumnsToDataset;
import com.fulmicotone.spark.java.app.processor.TimeDatasetProcessor;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.time.LocalDateTime;

public  class AddDateColumnsProcessor extends TimeDatasetProcessor {


    public int dayoffset=0;

    public AddDateColumnsProcessor(){ }

    public AddDateColumnsProcessor(int dayoffset){ this.dayoffset=dayoffset; }

    @Override
    public Dataset<Row> apply(Dataset dsupp, LocalDateTime scheduledTime, LocalDateTime executionTIme) {

        return new TimeColumnsToDataset().apply(scheduledTime.minusDays(dayoffset),dsupp);
    }
}