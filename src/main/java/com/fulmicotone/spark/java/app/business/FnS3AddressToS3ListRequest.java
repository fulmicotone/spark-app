package com.fulmicotone.spark.java.app.business;


import com.amazonaws.services.s3.model.ListObjectsV2Request;
import com.fulmicotone.spark.java.app.model.S3Address;

import java.util.function.Function;

public class FnS3AddressToS3ListRequest implements Function<S3Address, ListObjectsV2Request> {





    @Override
    public ListObjectsV2Request apply(S3Address s3Address) {

        return new ListObjectsV2Request().withBucketName(s3Address.getBucket())
                .withPrefix(s3Address
                        .getPrefix()
                        .lastIndexOf("/")==
                        s3Address.getPrefix().length()-1?s3Address.getPrefix():s3Address.getPrefix()+"/");

    }
}



