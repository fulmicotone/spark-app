package com.fulmicotone.spark.java.app;

import com.fulmicotone.spark.java.app.function.spark.DatasetSelectAll;
import com.fulmicotone.spark.java.app.function.spark.SparkSessionFactoryFn;
import com.fulmicotone.spark.java.app.processor.impl.SelectAllProcessor;
import org.apache.spark.sql.SparkSession;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.validation.constraints.AssertTrue;
import java.io.File;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public class SparkAppTest {

    static Logger  log= LoggerFactory.getLogger(SparkAppTest.class);

    private static SparkSession sparkSession;
    private static DatasetSupplier datasetSupplier;
    private static StepArg args;
    private static String resourcePath;

    @BeforeClass
    public static void setup(){

         resourcePath = new File(SparkAppTest
                .class.getClassLoader().getResource("input")
                .getPath()).getParent();


        log.info("resourcePath:"+resourcePath);

        args = StepArg.build(new String[]
                {"-i", resourcePath+"/input" ,
                        "-o", resourcePath + "/output",
                        "-cmd", "notNeedHere",
                        "-sdt", "2018-03-01T13:11:24",
                        "-edt", "2018-03-01T13:11:24",
                        "-env", "local"});

        sparkSession= new SparkSessionFactoryFn().apply(args );

        datasetSupplier=DatasetSupplier.create(sparkSession,args,sparkSession.read().option("header",true)
                .csv(resourcePath+ "/input/players/source.csv"));
    }



    @Test
    public void pathDecoderTest(){

        AtomicInteger atomicInteger=new AtomicInteger(-1);
        PathDecoder pathDecoder=new PathDecoder(args);

        List<String> results = Arrays.asList(
                pathDecoder.getAsString(PathDecoder.Direction.input, "players"),
                pathDecoder.getInputPath(args.scheduledDateTime, "players", 0),
                pathDecoder.getInputAWSPath(args.scheduledDateTime, "players", 0),
                pathDecoder.getOutputAWSPath(args.scheduledDateTime, "players", 0),
                pathDecoder.getOutputPath(args.scheduledDateTime, "players", 0),
                pathDecoder.getOutputPath(args.scheduledDateTime, "players", 2)
        ).stream()

                .map(r->r.replace(resourcePath,""))

                .collect(Collectors.toList());

        Arrays.asList("/input/players",
                "/input/players/year=2018/month=03/day=01",
                "/input/players/2018/03/01",
                "/output/players/2018/03/01",
                "/output/players/year=2018/month=03/day=01",
                "/output/players/year=2018/month=03/day=01/*/*")
        .forEach(expectedResult-> {
            String result = results.get(atomicInteger.incrementAndGet());


            Assert
            .assertTrue(result+ " is different of expected:"+expectedResult,result.equals(expectedResult));
        });

    }

    @Test
    public void datasetSelectAllTest(){


        datasetSupplier.map(new SelectAllProcessor(),new String[]{"Weight"}).get().collectAsList().stream()
                .forEach(r-> Assert.assertTrue(r.size()==5));

    }

    public void datasetTest(){}


    public void getSparkParallelismTest(){}



    public void newStepInstance(){}




    public void SparkSessionFactory(){}




}
