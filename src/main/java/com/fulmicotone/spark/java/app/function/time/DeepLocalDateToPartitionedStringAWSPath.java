package com.fulmicotone.spark.java.app.function.time;

import java.text.DecimalFormat;
import java.time.LocalDateTime;
import java.util.function.Function;

public class DeepLocalDateToPartitionedStringAWSPath implements Function<LocalDateTime,String> {
    @Override
    public String apply(LocalDateTime ldt) {
        DecimalFormat mFormat= new DecimalFormat("00");
        return   ldt.getYear()+"/"
                + mFormat.format(Double.valueOf(ldt.getMonth().getValue()))+"/"
                + mFormat.format(ldt.getDayOfMonth())+"/"
                +mFormat.format(ldt.getHour());

    }
}
