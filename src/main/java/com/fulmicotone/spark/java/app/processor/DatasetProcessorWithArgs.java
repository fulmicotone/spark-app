package com.fulmicotone.spark.java.app.processor;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.util.function.BiFunction;


public abstract class DatasetProcessorWithArgs<T> implements BiFunction<Dataset,T[], Dataset> { }