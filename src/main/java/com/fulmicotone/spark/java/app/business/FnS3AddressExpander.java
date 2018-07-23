package com.fulmicotone.spark.java.app.business;


import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.fulmicotone.spark.java.app.function.FnS3AddressConcat;
import com.fulmicotone.spark.java.app.function.PredicateS3AddressExist;
import com.fulmicotone.spark.java.app.function.time.DeepLocalDateToPartitionedStringAWSPath;
import com.fulmicotone.spark.java.app.function.time.DeepLocalDateToPartitionedStringPath;
import com.fulmicotone.spark.java.app.function.time.LocalDateToPartitionedStringPath;
import com.fulmicotone.spark.java.app.function.time.LocalDateToPartitionedStringAWSPath;
import com.fulmicotone.spark.java.app.model.S3Address;

import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public class FnS3AddressExpander implements Function<S3AddressExpander,List<S3Address>> {


 FnS3AddressExpander() { }

    @Override
    public List<S3Address> apply(S3AddressExpander s3AddressExpander) {


        boolean isHoursPrecision = s3AddressExpander.chronUnit.equals(ChronoUnit.HOURS);

        Stream<S3Address> y = IntStream.rangeClosed(1, s3AddressExpander.period)
                .mapToObj(i -> s3AddressExpander.localDateTime.minus(i, s3AddressExpander.chronUnit))
                .map((s3AddressExpander.isAmazonFormat ?
                        isHoursPrecision? new DeepLocalDateToPartitionedStringAWSPath() :
                                          new LocalDateToPartitionedStringAWSPath() :
                        isHoursPrecision ?
                                new DeepLocalDateToPartitionedStringPath() :
                                new LocalDateToPartitionedStringPath()))
                .map(partitionKey -> new FnS3AddressConcat().apply(s3AddressExpander.rootAddress, partitionKey))
                .map(S3Address::goWildDeep);

        if(s3AddressExpander.checkIfExist){
            y=y.filter(a-> new PredicateS3AddressExist()
                    .test(a, AmazonS3ClientBuilder.defaultClient()));
        }
        return y.collect(Collectors.toList());
    }
}



