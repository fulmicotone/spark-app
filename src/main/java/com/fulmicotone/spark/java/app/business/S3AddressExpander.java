package com.fulmicotone.spark.java.app.business;


import com.fulmicotone.spark.java.app.model.S3Address;

import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Objects;

/**
 * this objects is used for expand an s3address in a list of addresses.
 * This happens transforming the input adding a time partition prefix
 * example
 * input:
 * s3://mybucket  period 3 days from 03/01/2018
 * output:
 * s3://mybucket/2018/01/01
 * s3://mybucket/2018/01/02
 * s3://mybucket/2018/01/03
 */
public class S3AddressExpander {

    protected final ChronoUnit chronUnit;
    protected int period;
    protected LocalDateTime localDateTime;
    protected boolean isAmazonFormat;
    protected boolean checkIfExist;
    protected S3Address rootAddress;


    private S3AddressExpander(
            LocalDateTime localDateTime,
            int period,
            ChronoUnit chronUnit,
            boolean isAmazonFormat,
            boolean checkIfExist,
            S3Address rootAddress) {
        this.localDateTime = localDateTime;
        this.period = period;
        this.chronUnit=chronUnit;
        this.isAmazonFormat = isAmazonFormat;
        this.checkIfExist = checkIfExist;
        this.rootAddress = rootAddress;
    }

    public static Builder newOne(){ return new Builder(); }

    public List<S3Address> doExpansion(){
        return new FnS3AddressExpander().apply(this);
    }

    public static class Builder{

        private LocalDateTime localDateTime;
        private boolean isAmazonFormat=true;
        private boolean checkIfExist=false;
        private S3Address rootAddress;
        private int period;
        private ChronoUnit unit;


        private Builder(){ }

        public Builder onPeriod(int val, ChronoUnit unit){
            this.period =val;
            this.unit=unit;
            return this;
        }

        public Builder startFrom(LocalDateTime localDateTime){
            this.localDateTime=localDateTime;
            return this;
        }

        public Builder sourcePartitionedAsAWS(boolean isAWS){
            this.isAmazonFormat=isAWS;
            return this;
        }

        public Builder sourceBucketIsPartitioned(){
            this.isAmazonFormat=false;
            return this;
        }

        public Builder checkExistenceOnS3(){
            this.checkIfExist=true;
            return  this;
        }

        public Builder withSource(S3Address rootAddress){
            this.rootAddress=rootAddress;
            return this;
        }

        public S3AddressExpander create(){
            Objects.requireNonNull(rootAddress,"s3 root address is required!" );
            assert this.period >0;
            assert this.rootAddress.isWild()==false;
            return new S3AddressExpander(
                    this.localDateTime,
                    this.period,
                    this.unit,
                    isAmazonFormat,
                    checkIfExist,
                    rootAddress);
        }

    }
}
