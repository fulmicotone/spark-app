package com.fulmicotone.spark.java.app.utils;


import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.OptionDef;
import org.kohsuke.args4j.spi.OneArgumentOptionHandler;
import org.kohsuke.args4j.spi.Setter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;


public  class LocalDatetimeHandler extends OneArgumentOptionHandler<LocalDateTime> {

    private final Logger logger= LoggerFactory.getLogger(LocalDatetimeHandler.class);

    public LocalDatetimeHandler(CmdLineParser parser, OptionDef option, Setter<? super LocalDateTime> setter) {
        super(parser, option, setter);
    }

    @Override
    protected LocalDateTime parse(String argument) throws NumberFormatException {

        argument=argument.toLowerCase();
        logger.debug(argument);
       if(argument.charAt(argument.length()-1)!='z'){argument+="z";}
        return Instant.parse(argument
                .toLowerCase()).atOffset(ZoneOffset.UTC).toLocalDateTime();
    }

}
