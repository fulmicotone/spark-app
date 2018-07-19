package com.fulmicotone.spark.java.app.function.time;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.text.DecimalFormat;
import java.time.LocalDateTime;
import java.util.function.BiFunction;

import static org.apache.spark.sql.functions.lit;

public class DeepTimeColumnsToDataset implements BiFunction<LocalDateTime,Dataset<Row>,Dataset<Row>> {
    @Override
    public Dataset<Row> apply(LocalDateTime ldt, Dataset<Row> ds) {
        DecimalFormat mFormat = new DecimalFormat("00");
        return ds.withColumn("year", lit(ldt.getYear()))
                .withColumn("month", lit(mFormat.format(ldt.getMonth().getValue())))
                .withColumn("day", lit(mFormat.format(ldt.getDayOfMonth())))
                .withColumn("hour", lit(mFormat.format(ldt.getHour())));

    }

}
