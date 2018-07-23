package com.fulmicotone.spark.java.app.business;


import com.fulmicotone.spark.java.app.model.S3Address;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.BiFunction;
import java.util.stream.Collectors;

/**
 * this function allows to convert any dataset of row to parquet
 */
public class FnParquetConverter implements BiFunction<SparkSession,ParquetConverterRequest,Boolean> {


    private static Logger log=LoggerFactory.getLogger(FnParquetConverter.class);

     FnParquetConverter() { }

    @Override
    public Boolean apply(SparkSession sp,
                      ParquetConverterRequest parquetConverter) {


        Dataset<Row> rowDataset= sp
                .read()
                .schema(parquetConverter.schema)
                .format(parquetConverter.format).load(parquetConverter.
                        inputs
                        .stream()
                        .map(S3Address::toString)
                        .collect(Collectors.toList()).toArray(new String[]{}));


        if(parquetConverter.partitionSize!=-1){  rowDataset=rowDataset.coalesce(parquetConverter.partitionSize);}

        parquetConverter.transformation.apply(rowDataset)
                      .write()
                     .partitionBy(parquetConverter.partitions)
                     .mode(SaveMode.Append)
                     .parquet(parquetConverter.output.toString());

        return true;
    }

}

