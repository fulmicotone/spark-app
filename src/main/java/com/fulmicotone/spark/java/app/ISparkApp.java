package com.fulmicotone.spark.java.app;

@FunctionalInterface
public interface ISparkApp<T extends String> {


    void run(T[] args) throws Exception;
}
