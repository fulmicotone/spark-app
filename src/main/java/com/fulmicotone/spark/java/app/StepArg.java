package com.fulmicotone.spark.java.app;


import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fulmicotone.spark.java.app.utils.EnviromentHandler;
import com.fulmicotone.spark.java.app.utils.Enviroments;
import com.fulmicotone.spark.java.app.utils.LocalDatetimeHandler;
import org.kohsuke.args4j.Argument;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.kohsuke.args4j.spi.StringArrayOptionHandler;
import org.slf4j.LoggerFactory;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class StepArg {

    private static org.slf4j.Logger log = LoggerFactory.getLogger(StepArg.class);

    private CmdLineParser cmdLineParser;

    private Optional<CmdLineException> optException = Optional.empty();

    private StepArg() {
    }

    @Option(name = "-i", usage = "resources input folder", required = false)
    public String inputPath;
    @Option(name = "-o", usage = "resources output run something", required = false)
    public String outputPath;
    @Option(name = "-cmd", usage = "command to run")
    public String command;
    @Option(name = "-sdt", usage = "scheduled Date", handler = LocalDatetimeHandler.class, required = false)
    public LocalDateTime scheduledDateTime;
    @Option(name = "-edt", usage = "execution Date", handler = LocalDatetimeHandler.class, required = false)
    public LocalDateTime executionDateTime;
    @Option(name = "-env", usage = "enviroment prod or dev", handler = EnviromentHandler.class, required = false)
    public Enviroments enviroment = Enviroments.prod;

    @Argument(handler = StringArrayOptionHandler.class)
    public List<String> options = new ArrayList<String>();

    public static StepArg build(String[] args) {

        StepArg sa = new StepArg();

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


    public String asJson() {
        try {
            return new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT).writeValueAsString(this);

        } catch (JsonProcessingException e) {
            e.printStackTrace();
            return null;
        }
    }

}
