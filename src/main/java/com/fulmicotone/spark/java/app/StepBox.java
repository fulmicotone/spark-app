package com.fulmicotone.spark.java.app;


import org.apache.spark.sql.SparkSession;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Optional;

/**
 * class used for testing purpose
 */
public class StepBox {


    private static StepBox instance;



    private Step s;
    private StepArg a;
    private SparkSession ss;

    private StepBox( ){ }




    public static StepBox newTest(SparkSession ss, StepArg a){

        StepBox stb = Optional.ofNullable(StepBox.instance)
                .orElseGet(() -> (StepBox.instance = new StepBox()));
        stb.a=a;
        stb.ss=ss;
        stb.s=Step.newStepBy(a);
        return stb;
    }




    public  StepBox runOnly (){

        try{
                Method method = this.s.getClass().getDeclaredMethod("run", StepArg.class,SparkSession.class);
                method.setAccessible(true);
                method.invoke(this.s, this.a,this.ss);
        } catch(NoSuchMethodException e){
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        } catch (InvocationTargetException e) {
            e.printStackTrace();
        }


        return this;

    }


    public  StepBox fullRun (){

        try{
            Method method = this.s.getClass().getSuperclass().getDeclaredMethod("execute",
                    SparkSession.class);

            method.setAccessible(true);
            method.invoke(this.s,  this.ss);
        } catch(NoSuchMethodException e){
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        } catch (InvocationTargetException e) {
            e.printStackTrace();
        }

        return this;

    }



    public Step getStep(){ return this.s;};



}
