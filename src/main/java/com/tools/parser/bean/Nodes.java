package com.tools.parser.bean;

public class Nodes {
    private String name;

    public Nodes() {

    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    @Override
    public String toString() {
        return "{" +
                "\"name\":\"" + name +
                "\"}";
    }
}
