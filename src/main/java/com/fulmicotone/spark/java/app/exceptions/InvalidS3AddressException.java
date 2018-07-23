package com.fulmicotone.spark.java.app.exceptions;

public class InvalidS3AddressException extends  RuntimeException{


    public InvalidS3AddressException(String message) {
        super(message);
    }
}
