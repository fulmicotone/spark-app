package com.fulmicotone.spark.java.app.function.time;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.text.DecimalFormat;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.function.BiFunction;
import java.util.function.Function;

import static org.apache.spark.sql.functions.lit;

public class ISO8601StringToLocalDate implements Function<String,LocalDateTime> {


    @Override
    public LocalDateTime apply(String iso8601String) {
        return  LocalDateTime.parse(iso8601String.toString(), DateTimeFormatter.ISO_DATE_TIME);


    }
}
