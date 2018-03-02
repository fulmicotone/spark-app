package com.fulmicotone.spark.java.app.processor.impl;

import com.fulmicotone.spark.java.app.function.spark.DatasetSelectAll;
import com.fulmicotone.spark.java.app.processor.DatasetProcessor;
import com.fulmicotone.spark.java.app.processor.DatasetProcessorWithArgs;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public  class SelectAllProcessor extends DatasetProcessorWithArgs<String> {


    @Override
    public Dataset<Row> apply(Dataset<Row> rowDataset, String[] exludeCols) {
        return new DatasetSelectAll().apply(rowDataset,exludeCols);
    }
}