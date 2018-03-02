package com.fulmicotone.spark.java.app;


import com.fulmicotone.spark.java.app.exceptions.UnknownCommandException;
import com.fulmicotone.spark.java.app.function.spark.NewStepInstance;
import com.fulmicotone.spark.java.app.utils.AppPropertiesProvider;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.Properties;


/**
 * root class represents a spark single step
 * the start point to extends a new step.
 *
 * through its principal method newStepBy
 * we can instantiate a new step by the command name
 * for example
 * the command "getRevenue"  will instance a class
 * GetRevenueStep input the package
 */
public abstract class Step implements Serializable{

    protected Logger log= LoggerFactory.getLogger(this.getClass());
    protected StepArg arg;
    private Properties appProp=new AppPropertiesProvider().get();
    private PathDecoder pathDecoder;

    protected Step(){}

    public static Step newStepBy(StepArg arguments) throws UnknownCommandException {
        try {
            return new StepBuilder(arguments).build();
        } catch (Exception e) {
            throw
                    new UnknownCommandException(String
                            .format("%s command not allowed",
                                    arguments.command));
        }
    }

    protected Properties appProperties(){ return this.appProp;}

    /**
     *
     * @param key
     * @param format csv,json,parquet
     * @return read files from input bucket on the key indicated
     * it means that if we received the parameter -i s3://resources and as scheduled date 10/01/1987
     * passing parameter dir hit we'll get back s3://resources/hit/year=1987/month=01/day=10
     */
    public DatasetSupplier readOnDate(String key,
                                      String format,
                                      int wildDeepLevel,
                                      boolean awsStreamFolder,
                                      StructType... structType){

        return  DatasetSupplier.read(
                awsStreamFolder?pathDecoder.getInputAWSPath(arg.scheduledDateTime, key, wildDeepLevel):
                        pathDecoder.getInputPath(arg.scheduledDateTime, key, wildDeepLevel),
                format,
               this,
                SparkSession.getActiveSession().get(),structType);
    }


    public void run(SparkSession session){ this.run(this.arg,session);}

    protected abstract  void run(StepArg arg, SparkSession sparkSession);

    private static class StepBuilder{

        private final StepArg arguments;

        protected  StepBuilder(StepArg arguments)  { this.arguments=arguments; }

        public Step build() throws Exception {
            Step step= new NewStepInstance().apply(arguments.command);
            step.arg=arguments;
            step.pathDecoder=new PathDecoder(arguments);
            return step;

        }

    }

}
