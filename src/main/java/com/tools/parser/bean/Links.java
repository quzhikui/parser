package com.tools.parser.bean;

public class Links {
    private String source;
    private String target;
    private int value;

    public Links() {
    }

    public String getSource() {
        return source;
    }

    public void setSource(String source) {
        this.source = source;
    }

    public String getTarget() {
        return target;
    }

    public void setTarget(String target) {
        this.target = target;
    }

    public int getValue() {
        return value;
    }

    public void setValue(int value) {
        this.value = value;
    }
    public void setAllValue(String source,String target,int value) {
        this.source = source;
        this.target = target;
        this.value = value;
    }
    @Override
    public String toString() {
        return "{" +
                "\"source\":\"" + source +
                "\",\"target\":\"" + target +
                "\",\"value\":\"" + value +
                "\"}";
    }
}
