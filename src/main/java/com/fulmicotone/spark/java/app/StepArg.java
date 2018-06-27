package com.fulmicotone.spark.java.app;


import com.fulmicotone.spark.java.app.utils.Enviroments;

import java.io.Serializable;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class StepArg  implements Serializable {

    public StepArg() { }

    public String inputPath;
    public String outputPath;
    public String command;
    public LocalDateTime scheduledDateTime;
    public LocalDateTime executionDateTime;
    public Enviroments enviroment = Enviroments.prod;
    public List<String> options = new ArrayList<String>();

    public HashMap<String, String> getOptionsAsMap() {
        return optionsAsMap;
    }

    public void setOptionsAsMap(HashMap<String, String> optionsAsMap) {
        this.optionsAsMap = optionsAsMap;
    }

    public HashMap<String,String> optionsAsMap=new HashMap<>();

    public String getInputPath() {
        return inputPath;
    }

    public void setInputPath(String inputPath) {
        this.inputPath = inputPath;
    }

    public String getOutputPath() {
        return outputPath;
    }

    public void setOutputPath(String outputPath) {
        this.outputPath = outputPath;
    }

    public String getCommand() {
        return command;
    }

    public void setCommand(String command) {
        this.command = command;
    }

    public LocalDateTime getScheduledDateTime() {
        return scheduledDateTime;
    }

    public void setScheduledDateTime(LocalDateTime scheduledDateTime) {
        this.scheduledDateTime = scheduledDateTime;
    }

    public LocalDateTime getExecutionDateTime() {
        return executionDateTime;
    }

    public void setExecutionDateTime(LocalDateTime executionDateTime) {
        this.executionDateTime = executionDateTime;
    }

    public Enviroments getEnviroment() {
        return enviroment;
    }

    public void setEnviroment(Enviroments enviroment) {
        this.enviroment = enviroment;
    }

    public List<String> getOptions() {
        return options;
    }

    public void setOptions(List<String> options) {
        this.options = options;
    }

}
