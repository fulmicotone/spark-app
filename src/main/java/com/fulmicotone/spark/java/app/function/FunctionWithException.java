package com.fulmicotone.spark.java.app.function;

@FunctionalInterface
public interface FunctionWithException<T, R> { R apply(T t) throws Exception;}
