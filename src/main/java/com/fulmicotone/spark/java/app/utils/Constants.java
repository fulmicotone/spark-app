package com.fulmicotone.spark.java.app.utils;

public class Constants {
    public static  final String[] TIME_COLUMNS=new String[]{"year","month","day"};
    public static final String[] DEEP_TIME_COLUMNS=new String[]{"year","month","day","hour"};



    public static class Spark{


        public static String    SPARK_JOB_GROUP_ID = "spark.jobGroup.id";
        public static String   SPARK_JOB_INTERRUPT_ON_CANCEL = "spark.job.interruptOnCancel";
        public static String   RDD_SCOPE_KEY = "spark.rdd.scope";
        public static String   RDD_SCOPE_NO_OVERRIDE_KEY = "spark.rdd.scope.noOverride";
        public static String  SPARK_JOB_DESCRIPTION = "spark.job.description";


    }



}
