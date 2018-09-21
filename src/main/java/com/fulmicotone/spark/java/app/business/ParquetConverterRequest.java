package com.fulmicotone.spark.java.app.business;


import com.fulmicotone.spark.java.app.model.S3Address;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;

/**
 * this Object is used for reading a file
 * from input s3 address or a list of and write
 * it in parquet partitioned by columns
 */
public class ParquetConverterRequest implements Serializable {

    protected final int partitionSize;
    protected String format;
    protected String[] partitions;
    protected StructType schema;
    protected S3Address output;
    protected List<S3Address> inputs;
    protected Function<Dataset<Row>,Dataset<Row>> transformation;

    public ParquetConverterRequest(String format,
                                   String[] partitions,
                                   int partitionSize,
                                   StructType schema,
                                   List<S3Address> inputs,
                                   S3Address output,
                                   Function<Dataset<Row>,
                                   Dataset<Row>> transformation
                            ) {
        this.format = format;
        this.partitions = partitions;
        this.schema = schema;
        this.output = output;
        this.inputs = inputs;
        this.transformation=transformation;
        this.partitionSize=partitionSize;
    }




    public static Builder newOne(){return new Builder();}


    public Boolean doConvert(SparkSession spark){

        return new FnParquetConverter().apply(spark,this);

    }

    public static class Builder{

        private FileFormat format= FileFormat.json;
        private int partitionSize = -1;
        private String[]  partitions= new String[]{};
        private Optional<StructType> structTypeOpt=Optional.empty();
        private S3Address output;
        private List<S3Address> inputs;
        private Function<Dataset<Row>, Dataset<Row>> transformation=dataset->dataset;

        private Builder(){ }

        /**
         * list of s3 bucket containing original source files
         * @param addresses
         * @return
         */
        public Builder from(List<S3Address> addresses){this.inputs=addresses; return this;}

        public Builder from(S3Address address){this.inputs=Arrays.asList(address);return this;}

        /**
         *
         * @param address output s3 bucket
         * @return
         */
        public Builder to(S3Address address){this.output=address;return this;}

        /**
         *
         * @param format file format input
         * @return
         */
        public Builder format(FileFormat format){ this. format=format;return this;}

        /**
         * transformation function source=> target
         * @param fn
         * @return
         */
        public Builder withTransformation(Function<Dataset<Row>,Dataset<Row>> fn){ this.transformation=fn;return this;}

        public Builder partitionBy(String [] partitions){ this.partitions=partitions;return this;}

        public Builder withPartitionSize(int partitionSize){ this.partitionSize=partitionSize;return this; }

        public Builder withSchema(StructType schema){ this.structTypeOpt=Optional.of(schema);return this;}

        public ParquetConverterRequest create(){

            Objects.requireNonNull(inputs, "s3 address input is required");
            Objects.requireNonNull(output, "s3 address output is required");
            return new ParquetConverterRequest(
                    this.format.name(),
                    this.partitions ,
                    this.partitionSize,
                    structTypeOpt.orElse(null),
                    inputs,
                    output,
                    this.transformation);
        }
    }
}

