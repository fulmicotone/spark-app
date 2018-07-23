package com.fulmicotone.spark.java.app.function;


import com.fulmicotone.spark.java.app.model.S3Address;

import java.util.function.BiFunction;

public class FnS3AddressConcat implements BiFunction<S3Address,String ,S3Address> {


    @Override
    public S3Address apply(S3Address s3Address,String prefix) {

        return
                new S3Address((s3Address
                        .toString()
                        .lastIndexOf("/")==s3Address.toString().length()-1?
                        s3Address.toString():
                        s3Address.toString()+"/")+prefix);
    }
}



