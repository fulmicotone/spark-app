package com.fulmicotone.spark.java.app.business;


import com.fulmicotone.spark.java.app.model.S3Address;

import java.util.List;
import java.util.function.Function;

/**
 * extract Weight in bytes of a s3bucket and its object
 */
public class FnS3ListWeigh implements Function<List<S3Address>,Long> {

    @Override
    public Long apply(List<S3Address> s3Addresses) {

        return s3Addresses.stream()
                .peek(System.out::println)
                .map(new FnS3AddressToS3ListRequest() )
                .map(new FnS3ListObjectRequestToResults())
                .flatMap(rlist->rlist.stream())
                .flatMap(r->r.getObjectSummaries().stream())
                .map(s->s.getSize())
                .reduce((a, b) -> a+b).get();


    }
}



