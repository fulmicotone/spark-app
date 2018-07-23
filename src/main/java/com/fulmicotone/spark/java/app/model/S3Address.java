package com.fulmicotone.spark.java.app.model;


import com.fulmicotone.spark.java.app.exceptions.InvalidS3AddressException;

import java.io.Serializable;
import java.util.stream.IntStream;

public   class S3Address implements Serializable {

 final static String protocol_prefix ="s3://";
 final static String wild_path="/*";
 final String bucket;
 String prefix="";
 int deepWild;

 public S3Address(String address){
     if(!address.startsWith(protocol_prefix)){throw new InvalidS3AddressException("s3 address must start with s3://"); }
     this.bucket=address.substring(protocol_prefix.length(),
             address.indexOf("/",protocol_prefix.length())!=-1?
             address.indexOf("/",protocol_prefix.length()):address.length());

     if(address.length()> protocol_prefix.length()+bucket.length()) {
         String tmp = address.substring(protocol_prefix.length() + bucket.length() + 1, address.length());
         int len = tmp.length();
         tmp = tmp.replace(wild_path, "");
         this.prefix = len != tmp.length() && tmp.substring(tmp.length() - 1, tmp.length()).equals("/") ? tmp.substring(0, tmp.length() - 1) : tmp;
         deepWild = (len - this.prefix.length()) / wild_path.length();
     }
 }


    public String getBucket() {
        return bucket;
    }

    public String getPrefix() {
        return prefix;
    }


    public S3Address goWildDeep(){ this.deepWild++; return this;}


    public boolean isWild(){ return  deepWild>0;}

    @Override
    public String toString() {
        return protocol_prefix+bucket+"/"+prefix+IntStream
                .range(0,deepWild ).mapToObj(i->wild_path)
                .reduce((s1,s2)->s1+s2).orElse("");
    }
}
