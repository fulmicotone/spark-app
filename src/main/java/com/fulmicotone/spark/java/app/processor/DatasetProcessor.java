package com.fulmicotone.spark.java.app.processor;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.util.function.Function;


public abstract class DatasetProcessor implements Function<Dataset<Row>, Dataset<Row>> { }