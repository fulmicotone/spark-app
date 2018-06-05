package com.fulmicotone.spark.java.app.function;

import com.fulmicotone.spark.java.app.DatasetSupplier;
import com.fulmicotone.spark.java.app.StepArg;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;

import java.io.*;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.List;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.Function;

public class Functions {

    public static void  writeFileOnHadoop(Configuration config, InputStream is, String filename) throws IOException {

        FileSystem fs = FileSystem.get(URI.create(filename), config);

        IOUtils.copyBytes(is,  fs.create(new Path(filename)), 4096, true);

    }

    public static InputStream  readFileAsStreamFromHadoop(Configuration hadoopConf, String filePath) throws IOException {

        FileSystem fs = FileSystem.get(URI.create(filePath), hadoopConf);
        FSDataInputStream in = null;
        in = fs.open(new Path(filePath));
         IOUtils.copyBytes(in, System.out, 4096, false);
         return in;

    }


    public static Function<ByteArrayOutputStream,InputStream> FROM_BAOS_TO_IS=(baos)->new ByteArrayInputStream(baos.toByteArray());

    public static BiFunction<String,String,
                Function<String,Function<InputStream,File>>> FROM_IS_TO_TMP_FILE=
            (filename,ext)->(directory)->(is)-> {
                try {
                    java.io.File tmp = Optional.ofNullable(directory).isPresent()?
                            java.io.File.createTempFile(filename, ext, Paths.get(directory).toFile()):
                            java.io.File.createTempFile(filename, ext);
                    tmp.deleteOnExit();
                    Files.copy(is,tmp.toPath(), StandardCopyOption.REPLACE_EXISTING);
                    return tmp;
                } catch (FileNotFoundException e) { return null; } catch (IOException e) { return null; }
            };




    public static DatasetSupplier readByPathList(List<String> pathList,
                                              StepArg arg,
                                              SparkSession sp,
                                              String format,
                                              StructType ... structType){




        Dataset targetDS = null;

        for(String sourcePath:pathList){

            try {
                System.out.println("reading sourcesPath:" + sourcePath);

                targetDS = targetDS == null ? DatasetSupplier.read(sourcePath, format, arg,
                        sp, structType).get() : targetDS.union(DatasetSupplier.read(sourcePath, format, arg,
                        sp, structType).get());

            }catch (Exception ex){
                System.out.println("problem reading sourcesPath:" + sourcePath+" SKIPPED");
            }
        }

        return DatasetSupplier.create(sp,arg,targetDS);
    }
}
