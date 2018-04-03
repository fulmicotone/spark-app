package com.fulmicotone.spark.java.app.function.spark;

import org.apache.spark.sql.Dataset;
import scala.collection.JavaConversions;

import java.util.Arrays;
import java.util.List;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

import static org.apache.spark.sql.functions.col;

public class Select implements BiFunction<Dataset,String[],Dataset> {


    @Override
    public Dataset apply(Dataset ds, String[] cols) {

        return ds.select(JavaConversions.asScalaBuffer(Arrays.asList(cols)));
    }
}
