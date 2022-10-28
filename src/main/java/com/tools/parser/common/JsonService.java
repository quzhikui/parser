package com.tools.parser.common;

import com.tools.parser.bean.Energy;
import com.tools.parser.bean.Links;
import com.tools.parser.bean.Nodes;

import java.util.*;

public class JsonService {
    private List<Nodes> nodesList=new ArrayList<Nodes>();
    private List<Links> linksList=new ArrayList<Links>();
    public List<Nodes> getNodesList() {
        return nodesList;
    }
    //声明一个List类型的方法，并为其添加多个对象
    public void setNodesList(Nodes nodes) {
        this.nodesList.add(nodes);
    }

    public List<Links> getLinksList() {
        return linksList;
    }
    //声明一个List类型的方法，并为其添加多个对象
    public void setLinksList(Links links) {
        this.linksList.add(links);
    }

    //声明一个对象的方法，并为其添加单个对象
    public Energy getEnergy(){
        //先去重
        Set<Nodes> newNodesSet = new TreeSet<Nodes>(Comparator.comparing(o -> (o.getName())));
        Set<Links> newLinksSet = new TreeSet<Links>(Comparator.comparing(o -> (o.getSource()+""+o.getTarget())));
        newNodesSet.addAll(nodesList);
        newLinksSet.addAll(linksList);

        //new ArrayList<Nodes>(newNodesSet).forEach(System.out::println);
        //new ArrayList<Links>(newLinksSet).forEach(System.out::println);

        Energy eg = new Energy(newNodesSet, newLinksSet);
        return eg;
    }
}
