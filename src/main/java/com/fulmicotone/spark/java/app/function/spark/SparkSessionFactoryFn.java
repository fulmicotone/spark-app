package com.fulmicotone.spark.java.app.function.spark;


import com.fulmicotone.spark.java.app.StepArg;
import com.fulmicotone.spark.java.app.utils.Enviroments;
import org.apache.spark.sql.SparkSession;

import java.util.function.Function;

public class SparkSessionFactoryFn implements Function<StepArg,SparkSession> {


    @Override
    public SparkSession apply(StepArg arg) {


            SparkSession.Builder builder= SparkSession.builder().appName(arg.command+" - app");
            if(arg.enviroment== Enviroments.local){
                builder.master("local"); }
            SparkSession session= builder.getOrCreate();
          //  if(arg.enviroment==Enviroments.local){ session.sparkContext().setLogLevel(Level.ERROR.toString());};
            return session;
        };

}
