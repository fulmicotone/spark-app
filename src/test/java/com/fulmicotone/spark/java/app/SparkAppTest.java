package com.fulmicotone.spark.java.app;

import com.fulmicotone.spark.java.app.business.FnS3ListWeigh;
import com.fulmicotone.spark.java.app.business.S3AddressExpander;
import com.fulmicotone.spark.java.app.function.path.ApplyWildToS3Expander;
import com.fulmicotone.spark.java.app.function.spark.SparkSessionFactoryFn;
import com.fulmicotone.spark.java.app.function.time.DeepLocalDateToPartitionedStringAWSPath;
import com.fulmicotone.spark.java.app.function.time.DeepLocalDateToPartitionedStringPath;
import com.fulmicotone.spark.java.app.model.S3Address;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.Iterator;
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

        args = StepArgParser.build(new String[]
                {"-i", resourcePath+"/input" ,
                        "-o", resourcePath + "/output",
                        "-cmd", "notNeedHere",
                        "-sdt", "2018-03-01T13:11:24",
                        "-edt", "2018-03-01T13:11:24",
                        "-env","local"});

        sparkSession= new SparkSessionFactoryFn().apply(args );

        datasetSupplier=DatasetSupplier.create(sparkSession,args,sparkSession.read().option("header",true)
                .csv(resourcePath+ "/input/players/source.csv"));
    }


    @Test
    public void stepArgParameters(){

        String outpathVal="s3://prova-dino/client=myclient/container=custom/file=CUSTOM_171342_3442741_30.csv.gz";
        String filePathVal="s3://prova-dino/client=myclient/container=custom/file=CUSTOM_171342_3442741_30.csv.gz";
        String filenameVal="web_ciao-prova-x-*=.csv";
        String debugModeVal="false";
        String paramVal="paramVal";
        StepArg stepArg = StepArgParser.build(new String[]{
                "-sdt", "2018-11-05T16:42:26.828",
                "-edt", "2018-11-05T16:42:26.828",
                "-cmd", "com.mycompany.spark.steps.mystep.Step",
                "file_path="+filePathVal,
                "file_name="+filenameVal,
                "-o", outpathVal,
                "debug_mode="+debugModeVal,
                "param="+paramVal
        }).toAppArgs();

        Assert.assertTrue(stepArg.outputPath.equals(outpathVal));
        Assert.assertTrue(stepArg.optionsAsMap.get("file_name").equals(filenameVal));
        Assert.assertTrue(stepArg.optionsAsMap.get("file_path").equals(filePathVal));
        Assert.assertTrue(stepArg.optionsAsMap.get("param").equals(paramVal));
        Assert.assertTrue(stepArg.optionsAsMap.get("debug_mode").equals(debugModeVal));

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
    public void testS3AddressExpander(){

        LocalDateTime ltd = LocalDateTime.of(2018, 01, 10, 00, 00);

        Iterator<String> i1 = S3AddressExpander.newOne()
                .withSource(new S3Address("s3://ciao"))
                .onPeriod(5,ChronoUnit.DAYS)
                .startFrom(ltd).create()
                .doExpansion()
                .stream()
                .map(S3Address::toString).collect(Collectors.toList()).iterator();


        Iterator<String> i2 = S3AddressExpander.newOne()
                .withSource(new S3Address("s3://ciao"))
                .onPeriod(5,ChronoUnit.DAYS)
                .sourceBucketIsPartitioned()
                .startFrom(ltd).create()
                .doExpansion()
                .stream()
                .map(S3Address::toString)
                .collect(Collectors.toList()).iterator();


        Iterator<String> i3 = S3AddressExpander.newOne()
                .withSource(new S3Address("s3://ciao"))
                .onPeriod(5,ChronoUnit.HOURS)
                .sourceBucketIsPartitioned()
                .startFrom(ltd).create()
                .doExpansion()
                .stream()
                .map(S3Address::toString)
                .collect(Collectors.toList()).iterator();


        Iterator<String> i4 = S3AddressExpander.newOne()
                .withSource(new S3Address("s3://ciao"))
                .onPeriod(1,ChronoUnit.HOURS)
                .sourceBucketIsPartitioned()
                .startFrom(ltd)
                .sourcePartitionedAsAWS(true)
                .create()
                .doExpansion()
                .stream()
                .map(S3Address::toString)
                .collect(Collectors.toList()).iterator();


        Iterator<String> i5 = new ApplyWildToS3Expander().apply(S3AddressExpander.newOne()
                .withSource(new S3Address("s3://ciao"))
                .onPeriod(5, ChronoUnit.DAYS)
                .sourceBucketIsPartitioned()
                .startFrom(ltd).create(), 2)
                .stream()
                .map(S3Address::toString)
                .iterator();


        Iterator<String> i6 = new ApplyWildToS3Expander().apply(S3AddressExpander.newOne()
                .withSource(new S3Address("s3://ciao"))
                .onPeriod(0, ChronoUnit.DAYS)
                .sourceBucketIsPartitioned()
                .startFrom(ltd).create(), 2)
                .stream()
                .map(S3Address::toString)
                .peek(System.out::println)
                .iterator();

        Arrays.asList(
                "s3://ciao/2018/01/09/*",
                "s3://ciao/2018/01/08/*",
                "s3://ciao/2018/01/07/*",
                "s3://ciao/2018/01/06/*"
        ).stream().forEach(r-> Assert.assertTrue( r.equals(i1.next())));

        Arrays.asList("s3://ciao/year=2018/month=01/day=09/*",
                "s3://ciao/year=2018/month=01/day=08/*",
                "s3://ciao/year=2018/month=01/day=07/*",
                "s3://ciao/year=2018/month=01/day=06/*"
        ).stream().forEach(r-> Assert.assertTrue( r.equals(i2.next())));



        Arrays.asList("s3://ciao/year=2018/month=01/day=09/hour=23/*",
                "s3://ciao/year=2018/month=01/day=09/hour=22/*",
                "s3://ciao/year=2018/month=01/day=09/hour=21/*",
                "s3://ciao/year=2018/month=01/day=09/hour=20/*"
        ).stream().forEach(r-> Assert.assertTrue( r.equals(i3.next())));




        Arrays.asList("s3://ciao/2018/01/09/23/*"
        ).stream().forEach(r-> Assert.assertTrue( r.equals(i4.next())));



        Arrays.asList("s3://ciao/year=2018/month=01/day=09/*/*",
                "s3://ciao/year=2018/month=01/day=08/*/*",
                "s3://ciao/year=2018/month=01/day=07/*/*",
                "s3://ciao/year=2018/month=01/day=06/*/*"
        ).stream()

                .forEach(r-> Assert.assertTrue( r.equals(i5.next())));


        Arrays.asList("s3://ciao/year=2018/month=01/day=10/*/*"
        ).stream()

                .forEach(r-> Assert.assertTrue( r.equals(i6.next())));


    }



    @Test
    public void deepLocalDateTest(){


        Iterator<String> i = Arrays.asList(
                "2018/03/01/13",
                "year=2018/month=03/day=01/hour=13").iterator();

        Arrays.asList(
                new DeepLocalDateToPartitionedStringAWSPath().apply(args.scheduledDateTime),
                new DeepLocalDateToPartitionedStringPath().apply(args.scheduledDateTime)).stream()
                .peek(System.out::println)
                .forEach(i.next()::equals);
        
    }



    @Test
    public void stepBoxTest(){

        StepArg arg = StepArgParser.build(new String[]{
                "-i", "x",
                "-o", "fakeOutputPath",
                "-cmd", "com.fulmicotone.spark.java.app.MyStep",
                "-sdt", "2018-05-23T23:59:00",
                "-edt", "2018-05-23T23:59:00",
                "-env", "local",
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



    @Test
    public void testS3AddressObject(){

        String s3Path="s3://prod-audiencerate-aws-kinesis/firehose/raw/match-table/*/*/*";

        String s3Path2="s3://prod-audiencerate-aws-kinesis/firehose/raw/match-table";


        S3Address s3add = new S3Address(s3Path);

        S3Address s3add2 = new S3Address(s3Path2);

        Assert.assertTrue(String.format("expected %s found %s",s3Path,s3add.toString()),
                s3Path.equals(s3add.toString()));

        Assert.assertTrue(String.format("expected %s found %s",s3Path2,s3add2.toString()),
                s3Path2.equals(s3add2.toString()));



    }



 //   @Test
    public void testS3WighFunction(){



        LocalDateTime ltd = LocalDateTime.of(2018, 9, 26, 00, 00);

        List<S3Address> l = S3AddressExpander.newOne()
                .withSource(new S3Address("s3://prova/parquet-rrr-f/sync-ofperation"))
                .onPeriod(1, ChronoUnit.DAYS)
                .startFrom(ltd)
                .sourceBucketIsPartitioned()
                .create()
                .doExpansion()
                .stream()
                .peek(System.out::println)
                .collect(Collectors.toList());


        Long x = new FnS3ListWeigh().apply(l);



        System.out.println(x);



    }



}
