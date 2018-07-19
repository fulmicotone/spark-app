package com.fulmicotone.spark.java.app.function.time;

import java.text.DecimalFormat;
import java.time.LocalDateTime;
import java.util.function.Function;

public class DeepLocalDateToPartitionedStringPath implements Function<LocalDateTime,String> {
    @Override
    public String apply(LocalDateTime ldt) {
        DecimalFormat mFormat = new DecimalFormat("00");

        return    "year=" + ldt.getYear() + "/" +
                  "month=" + mFormat.format(Double.valueOf(ldt.getMonth().getValue())) + "/" +
                  "day=" + mFormat.format(ldt.getDayOfMonth())+"/"+
                  "hour="+mFormat.format(ldt.getHour());
    }
}
