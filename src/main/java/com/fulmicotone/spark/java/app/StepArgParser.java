package com.fulmicotone.spark.java.app;


import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fulmicotone.spark.java.app.utils.EnviromentHandler;
import com.fulmicotone.spark.java.app.utils.Enviroments;
import com.fulmicotone.spark.java.app.utils.LocalDatetimeHandler;
import org.apache.commons.beanutils.BeanUtils;
import org.apache.commons.lang.math.IntRange;
import org.kohsuke.args4j.Argument;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationTargetException;
import java.time.LocalDateTime;
import java.util.Optional;
import java.util.function.Predicate;
import java.util.stream.IntStream;

public class StepArgParser extends  StepArg {

    private static org.slf4j.Logger log = LoggerFactory.getLogger(StepArgParser.class);

    private CmdLineParser cmdLineParser;

    private Optional<CmdLineException> optException = Optional.empty();

    private final Predicate<String> isMapOption =(s)->s.split("=").length>=2;

    public StepArgParser() { }

    @Option(name = "-i", usage = "resources input folder", required = false)
    @Override
    public void setInputPath(String inputPath) {
        super.setInputPath(inputPath);
    }


    @Option(name = "-o", usage = "resources output run something", required = false)
    @Override
    public void setOutputPath(String outputPath) {
        super.setOutputPath(outputPath);
    }


    @Option(name = "-cmd", usage = "command to run")
    @Override
    public void setCommand(String command) {
        super.setCommand(command);
    }


    @Option(name = "-sdt", usage = "scheduled Date", handler = LocalDatetimeHandler.class, required = false)
    @Override
    public void setScheduledDateTime(LocalDateTime scheduledDateTime) {
        super.setScheduledDateTime(scheduledDateTime);
    }


    @Option(name = "-edt", usage = "execution Date", handler = LocalDatetimeHandler.class, required = false)
    @Override
    public void setExecutionDateTime(LocalDateTime executionDateTime) {
        super.setExecutionDateTime(executionDateTime);
    }


    @Option(name = "-env", usage = "enviroment prod or dev", handler = EnviromentHandler.class, required = false)
    @Override
    public void setEnviroment(Enviroments enviroment) {
        super.setEnviroment(enviroment);
    }

    @Argument
    public void setOptions(String opt) {

        if(isMapOption.test(opt)){
            String[] keyVal = opt.split("=");

            optionsAsMap.put(keyVal[0],IntStream.range(1,keyVal.length )
                    .mapToObj(i->keyVal[i]).reduce((x,y)->x+"="+y).get() );
            return;
        }
        options.add(opt);

    }




    public static StepArgParser build(String[] args) {

        StepArgParser sa = new StepArgParser();

        try {
            CmdLineParser parser = new CmdLineParser(sa);
            sa.cmdLineParser = parser;
            parser.parseArgument(args);
            log.debug("Arguments:{}", sa.asJson());
        } catch (CmdLineException e) {
            sa.optException = Optional.of(e);
        }
        return sa;
    }


    public void exitOnErrorPrintUsage() {

        if (this.optException.isPresent()) {
            cmdLineParser.printUsage(System.err);
            System.exit(0);
        }

    }


    public StepArg toAppArgs(){

        try {
                StepArg appArgs = new StepArg();
                BeanUtils.copyProperties(appArgs, this);
                return appArgs;
        } catch (IllegalAccessException e) {
            e.printStackTrace();
            return null;
        } catch (InvocationTargetException e) {
            e.printStackTrace();
            return null;
        }


    }


    public String asJson() {
        try {
            return new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT).writeValueAsString(this);

        } catch (JsonProcessingException e) {
            e.printStackTrace();
            return null;
        }
    }



}
