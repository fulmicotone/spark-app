package com.fulmicotone.spark.java.app.function.path;

import com.fulmicotone.spark.java.app.business.S3AddressExpander;
import com.fulmicotone.spark.java.app.model.S3Address;

import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class ApplyWildToS3Expander implements BiFunction<S3AddressExpander,Integer,List<S3Address>> {

    @Override
    public List<S3Address> apply(S3AddressExpander s3AddressExpander,Integer wildDeepLevel) {

       return  s3AddressExpander.doExpansion() .stream()
                .map(s3Addr-> {
                    AtomicReference<S3Address> s3AtomicAddr = new AtomicReference<>(s3Addr);
                    IntStream.range(1,wildDeepLevel ).forEach((i)->s3AtomicAddr.getAndUpdate(S3Address::goWildDeep));
                    return s3AtomicAddr.get();
                }).collect(Collectors.toList());
    }
}
