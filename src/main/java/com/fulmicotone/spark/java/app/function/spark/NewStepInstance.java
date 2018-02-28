package com.fulmicotone.spark.java.app.function.spark;

import com.fulmicotone.spark.java.app.Step;
import com.fulmicotone.spark.java.app.exceptions.UnknownCommandException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.Function;

public class NewStepInstance implements Function<String,Step> {


    Logger log= LoggerFactory.getLogger(NewStepInstance.class);

    @Override
    public Step apply(String className) {


        try {
            return (Step) Class.forName(className).newInstance();
        } catch (InstantiationException e) {
            log.error("command not found, InstantiationException ",className);
           throw new UnknownCommandException(e.toString());
        } catch (IllegalAccessException e) {
            log.error("command not found, IllegalAccessException  ",className);
            throw new UnknownCommandException(e.toString());
        } catch (ClassNotFoundException e) {
            log.error("command not found, ClassNotFoundException ",className);
            throw new UnknownCommandException(e.toString());
        }
    }
}
