package com.untils.parser.tools;

import com.untils.parser.common.Connections;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;

public class ImpalaExample {
    public static void main(String[] args) throws Exception {
        String str="ts_ss_view";
        System.out.println(str.substring(str.length()-5));
        //这个地址填写自己的impala server地址,默认端口为21050
        //格式为jdbc:impala://[Host]:[Port]/[Schema];[Property1]=[Value];[Property2]=[Value];...
        //默认连接default库
        String connectionUrl = "jdbc:impala://192.168.32.132:21050";
        //使用4.1版本
        String jdbcDriverName = "com.cloudera.impala.jdbc.Driver";
        //简单的一个查询语句
        String sqlStatement = "select * from default.quzk_temp;";
        Connections conn= new Connections();
        Connection con=conn.getImpalaConnection();

            Statement stmt = con.createStatement();
            //ResultSet rs = stmt.executeQuery(sqlStatement);
            boolean fs = stmt.execute("use sd_src");
            ResultSet rs = stmt.executeQuery("show tables");
            System.out.println("---begin query---");

            //打印输出
            while (rs.next()) {
                System.out.println(rs.getString(1));
            }
            System.out.println("---end query---");

    }
}