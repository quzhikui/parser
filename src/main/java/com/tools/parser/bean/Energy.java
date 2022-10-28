package com.tools.parser.bean;

import java.util.Set;

public class Energy {
    private Set<Nodes> nodes;
    private Set<Links> links;

    public Energy() {
    }

/*    public Energy(List<Nodes> nodes, List<Links> links) {
        this.nodes = nodes;
        this.links = links;
    }*/
    public Energy(Set<Nodes> nodes, Set<Links> links) {
        this.nodes = nodes;
        this.links = links;
    }
    public Set<Nodes> getNodes() {
        return nodes;
    }

    public void setNodes(Set<Nodes> nodes) {
        this.nodes = nodes;
    }

    public Set<Links> getLinks() {
        return links;
    }

    public void setLinks(Set<Links> links) {
        this.links = links;
    }

    @Override
    public String toString() {
        return "{" +
                "\"nodes\":" + nodes +
                ", \"links\":" + links +
                '}';
    }
}
