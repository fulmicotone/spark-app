package com.fulmicotone.spark.java.app.processor.impl;

import com.fulmicotone.spark.java.app.processor.DatasetProcessor;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public  class CacheDatasetProcessor extends DatasetProcessor {


    @Override
    public Dataset<Row> apply(Dataset<Row> rowDataset) {

        rowDataset.cache();
        rowDataset.count();
        return rowDataset;
    }
}