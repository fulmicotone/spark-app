package com.fulmicotone.spark.java.app.function;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.io.*;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.Function;

public class Functions {

    public static void  writeFileOnHadoop(Configuration config, InputStream is, String filename) throws IOException {

        FileSystem fs = FileSystem.get(URI.create(filename), config);

        IOUtils.copyBytes(is,  fs.create(new Path(filename)), 4096, true);

    }


    public static Function<ByteArrayOutputStream,InputStream> FROM_BAOS_TO_IS=(baos)->new ByteArrayInputStream(baos.toByteArray());

    private static BiFunction<String,String,
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
}
