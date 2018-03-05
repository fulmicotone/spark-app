package com.fulmicotone.spark.java.app;

import com.fulmicotone.spark.java.app.processor.DatasetProcessor;
import com.fulmicotone.spark.java.app.processor.DatasetProcessorWithArgs;
import com.fulmicotone.spark.java.app.processor.TimeDatasetProcessor;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDateTime;
import java.util.function.Supplier;


public class DatasetSupplier implements Supplier<Dataset> {


    protected final SparkSession session;
    protected final StepArg args;
    private Dataset<Row> dataset;

    protected DatasetSupplier(SparkSession session,
                              StepArg args,
                              Dataset<Row> ds) {
        this.session = session;
        this.dataset = ds;
        this.args = args;
    }


    public DatasetSupplier map(TimeDatasetProcessor processor) {
        return transform(processor.apply(this.dataset, this.args.scheduledDateTime, this.args.scheduledDateTime));

    }

    public DatasetSupplier map(DatasetProcessor processor) {
        return transform(processor.apply(this.dataset));
    }


    public <E> DatasetSupplier map(DatasetProcessorWithArgs<E> processor, E... args) {
        return transform(processor.apply(this.dataset, args));
    }


    private DatasetSupplier transform(Dataset<Row> dataset) {
        return create(this.session, this.args, dataset);
    }

    public static DatasetSupplier read(String path,
                                       String format,
                                       Step step,
                                       SparkSession session, StructType... schema) {


        return new DatasetSupplier(session, step.arg, schema.length > 1 ?
                session.read().schema(schema[0]).option("header", "true").format(format).load(path) :
                session.read().option("header", "true").format(format).load(path));
    }


    public static DatasetSupplier create(SparkSession session, StepArg args, Dataset<Row> dataset) {
        return new DatasetSupplier(session, args, dataset);
    }

    @Override
    public Dataset<Row> get() {
        return dataset;
    }


}