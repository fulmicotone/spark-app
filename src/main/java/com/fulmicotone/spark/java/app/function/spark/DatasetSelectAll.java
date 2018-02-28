package com.fulmicotone.spark.java.app.function.spark;

import org.apache.spark.sql.Dataset;
import scala.collection.JavaConversions;

import java.util.Arrays;
import java.util.List;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

import static org.apache.spark.sql.functions.col;

public class DatasetSelectAll implements BiFunction<Dataset,String[],Dataset> {


    @Override
    public Dataset apply(Dataset ds, String[] excludeCols) {
        final List<String> excludeColsList = Arrays.asList(excludeCols);
        return ds.select(JavaConversions.asScalaBuffer(Arrays.stream(ds.schema().fieldNames())
                .filter(fieldName->!excludeColsList.contains(fieldName))
                .map(fieldName->col(fieldName))
                .collect(Collectors.toList())).toSeq());
    }
}
