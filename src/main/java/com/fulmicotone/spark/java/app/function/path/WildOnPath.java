package com.fulmicotone.spark.java.app.function.path;

import java.util.function.BiFunction;

public class WildOnPath implements BiFunction<String,Integer,String> {
    @Override
    public String apply(String sourcePath, Integer deepLevel) {

        for(int i=0;i<deepLevel;i++){sourcePath+="/*";}
        return sourcePath;
    }
}
