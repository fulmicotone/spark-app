package com.fulmicotone.spark.java.app.business;


import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ListObjectsV2Request;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.fulmicotone.spark.java.app.model.S3Address;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

/**
 * warning
 * use it when you are sure that there's memory enough to contains the entire results
 */
public class FnS3ListObjectRequestToResults implements Function<ListObjectsV2Request,List<ListObjectsV2Result>> {


    @Override
    public List<ListObjectsV2Result> apply(ListObjectsV2Request req) {

       AmazonS3 s3 = AmazonS3ClientBuilder.defaultClient();
       List<ListObjectsV2Result> results=new ArrayList<>();

        ListObjectsV2Result r;
        do {
            r = s3.listObjectsV2(req);
            results.add(r);
            req.setContinuationToken(r.getNextContinuationToken());
        } while (r.isTruncated());

        return results;
    }
}



