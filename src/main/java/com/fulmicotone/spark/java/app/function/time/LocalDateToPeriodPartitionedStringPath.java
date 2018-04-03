package com.fulmicotone.spark.java.app.function.time;

import java.text.DecimalFormat;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.function.BiFunction;

public class LocalDateToPeriodPartitionedStringPath implements BiFunction<LocalDateTime,Integer,List<String>> {

    final static String format="year=%s/month=%s/day=%s";

    @Override
    public List<String> apply(LocalDateTime ldt,Integer days) {

        List<String> paths=new ArrayList<>();

        ldt = ldt.minusDays(days);

        final DecimalFormat mF = new DecimalFormat("00");

        for(int i=0;i<days;i++){
            ldt=ldt.plusDays(1);
            paths.add(String.format(format,ldt.getYear(),
                    mF.format(Double.valueOf(ldt.getMonthValue())),
                    mF.format(Double.valueOf(ldt.getDayOfMonth()))));
        }

        return paths;
    }


}
