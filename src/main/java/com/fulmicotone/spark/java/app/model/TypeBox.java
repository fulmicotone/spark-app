package com.fulmicotone.spark.java.app.model;

public class TypeBox<T> {

    private T t;

    public T get(){return this.t;}

    protected TypeBox(T t){ this.t =t;}

    public static <T>TypeBox wrap(T t){

        return new TypeBox(t);

    }

    @Override
    public String toString() {
        return t.toString();
    }
}
