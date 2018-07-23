package com.fulmicotone.spark.java.app.function;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.ListObjectsV2Request;

import com.fulmicotone.spark.java.app.model.S3Address;

import java.util.function.BiPredicate;

public class PredicateS3AddressExist implements BiPredicate<S3Address,AmazonS3> {
    @Override
    public boolean test(S3Address s3Address, AmazonS3 s3) {

        if(!s3.doesBucketExistV2(s3Address.getBucket())){return false;}
        ListObjectsV2Request req = new ListObjectsV2Request().withBucketName(s3Address.getBucket())
                .withPrefix(s3Address
                        .getPrefix()
                        .lastIndexOf("/")==
                        s3Address.getPrefix().length()-1?s3Address.getPrefix():s3Address.getPrefix()+"/");
        return s3.listObjectsV2(req).getKeyCount()>0;

    }

    }



