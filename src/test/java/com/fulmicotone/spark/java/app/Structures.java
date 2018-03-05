package com.fulmicotone.spark.java.app;

import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class Structures {


    public static StructType PLAYER=new StructType(new StructField[]{

            DataTypes.createStructField("Name", DataTypes.StringType,true),
            DataTypes.createStructField("Team", DataTypes.StringType,true),
            DataTypes.createStructField("Position", DataTypes.StringType,true),
            DataTypes.createStructField("Height", DataTypes.IntegerType,true),
            DataTypes.createStructField("Weight", DataTypes.IntegerType,true),
            DataTypes.createStructField("Age", DataTypes.FloatType,true)

    });
}
