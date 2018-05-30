package com.fulmicotone.spark.java.app;

import com.fulmicotone.spark.java.app.function.Functions;
import com.fulmicotone.spark.java.app.function.spark.SparkSessionFactoryFn;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
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
    public void dataSupplierTest(){


        Dataset<Player> ds = DatasetSupplier.read(resourcePath + "/input/players/source.csv",
                "csv", args, sparkSession,Structures.PLAYER).getAs(Player.class);
        
        Player player = ds.collectAsList().get(0);
        Assert.assertTrue(ds.count()==19);
        Assert.assertTrue(player.getName().equals("AdamDonachie"));
        Assert.assertTrue(player.getHeight().equals(74));
        Assert.assertTrue(player.getPosition().equals("Catcher"));
        Assert.assertTrue(player.getTeam().equals("BAL"));
        Assert.assertTrue(player.getAge()+"",player.getAge()==(22.99f ));
        Assert.assertTrue(player.getWeight().equals(180));

    }


    @Test
    public void localDateToPeriodTest(){


        List<String> results = Arrays.asList(
                "/input/players/year=2018/month=02/day=20/*",
                "/input/players/year=2018/month=02/day=21/*",
                "/input/players/year=2018/month=02/day=22/*",
                "/input/players/year=2018/month=02/day=23/*",
                "/input/players/year=2018/month=02/day=24/*",
                "/input/players/year=2018/month=02/day=25/*",
                "/input/players/year=2018/month=02/day=26/*",
                "/input/players/year=2018/month=02/day=27/*",
                "/input/players/year=2018/month=02/day=28/*",
                "/input/players/year=2018/month=03/day=01/*");

        //start date 2018 03 01
        PathDecoder pathDecoder=new PathDecoder(args);

        AtomicInteger atomicInteger=new AtomicInteger(-1);

        List<String> sourcesPath = pathDecoder.getInputPathsByPeriod(args.scheduledDateTime, "players", 10, 1);

        sourcesPath.stream()
                .map(r->r.replace(resourcePath,"")).
                 forEach(expectedResult-> {
            String result = results.get(atomicInteger.incrementAndGet());

            Assert
                    .assertTrue(result+ " is different of expected:"
                            +expectedResult,result.equals(expectedResult));
        });


        DatasetSupplier ds = Functions.readByPathList(sourcesPath, args, sparkSession, "csv");

        int size= (int) ds.get().count();
        Row lastRecord = ds.get().collectAsList().get(size - 1);
        Row firstRecord = ds.get().collectAsList().get(0);

        Assert.assertTrue("expected AdamDonachie  was:"+
                firstRecord.getString(0),firstRecord.getString(0).equals("AdamDonachie"));
        Assert.assertTrue(lastRecord.getString(0).equals("HaydenPenn"));
        Assert.assertTrue("size expected:"+19+" was:"+size,size==19);

    }



    @Test
    public void stepBoxTest(){



        StepArg arg = StepArg.build(new String[]{
                "-i", "x",
                "-o", "fakeOutputPath",
                "-cmd", "com.fulmicotone.spark.java.app.MyStep",
                "-sdt", "2018-05-23T23:59:00",
                "-edt", "2018-05-23T23:59:00",
                "-env","local",
                "o",
                "f"
        });


        Dataset fullLifeCycleResult = StepBox.newTest(sparkSession, arg)
                .fullRun().getStep().unwrapDataset()
                .get();


        Dataset onlyRunMethodResult = StepBox.newTest(sparkSession, arg)
                .runOnly().getStep().unwrapDataset()
                .get();

        List<String> rExpected1 = Arrays.asList("RUN");
        List<String> rExpected2 = Arrays.asList("AFTERRUN","RUN","BEFORERUN");


        Assert.assertTrue( onlyRunMethodResult.collectAsList().stream().allMatch(rExpected1::contains));

        Assert.assertTrue( fullLifeCycleResult.collectAsList().stream().allMatch(rExpected2::contains));

    }



}
