package com.fulmicotone.spark.java.app;


import com.fulmicotone.spark.java.app.function.spark.SparkSessionFactoryFn;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SparkMain implements ISparkApp<String>{

    Logger log= LoggerFactory.getLogger(SparkMain.class);

    @Override
    public void run(String[] args) throws Exception {


        StepArg appArgs = StepArg.build(args);

        appArgs.exitOnErrorPrintUsage();

        try (SparkSession spark=new SparkSessionFactoryFn().apply(appArgs)) {

            SparkSession.setActiveSession(spark);

            Step.newStepBy(appArgs).run(spark);

        } catch (Exception e) {

            log.error("error in SparkMain:"+appArgs.command);
            throw e;
        }


    }
}

