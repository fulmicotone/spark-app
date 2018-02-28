package com.fulmicotone.spark.java.app.utils;


import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.OptionDef;
import org.kohsuke.args4j.spi.OneArgumentOptionHandler;
import org.kohsuke.args4j.spi.Setter;


public  class EnviromentHandler extends OneArgumentOptionHandler<Enviroments> {


    public EnviromentHandler(CmdLineParser parser, OptionDef option, Setter<Enviroments> setter) {
        super(parser, option, setter);
    }
    @Override
    protected Enviroments parse(String s) throws NumberFormatException, CmdLineException {
        return Enviroments.valueOf(s);
    }
}
