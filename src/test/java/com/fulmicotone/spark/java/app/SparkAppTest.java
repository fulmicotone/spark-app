package com.fulmicotone.spark.java.app;

import com.fulmicotone.spark.java.app.function.spark.SparkSessionFactoryFn;
import com.fulmicotone.spark.java.app.processor.impl.SelectAllProcessor;
import org.apache.spark.sql.*;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.StructType;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
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

   // @Test
    public void datasetSelectAllTest(){


      /*  datasetSupplier.map(new SelectAllProcessor(),new String[]{"Weight"}).get().collectAsList().stream()
                .forEach(r-> Assert.assertTrue(r.size()==5));*/

    }

    @Test
    public void datasetTest(){


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

    public void getSparkParallelismTest(){}

    public void newStepInstance(){}

    public void SparkSessionFactory(){}

/**public static Function<ByteArrayOutputStream,InputStream> FROM_BAOS_TO_IS=(baos)->new ByteArrayInputStream(baos.toByteArray());**/

   /* Configuration hadoopConfig=new Configuration();

                log.info("connecting with: {}",hadoopConfig.get("fs.defaultFS"));*/

   /* public static void  writeFileToHadoop(Configuration config, InputStream is, String filename) throws IOException {

        FileSystem fs = FileSystem.get(URI.create(filename), config);

        IOUtils.copyBytes(is,  fs.create(new Path(filename)), 4096, true);



    }*/

}
