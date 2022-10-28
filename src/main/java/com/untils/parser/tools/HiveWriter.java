package com.untils.parser.tools;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.net.URI;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class HiveWriter {

        public static void main(String[] args) {
            List<List<String>> argList = new ArrayList<>();
            List<String> arg = new ArrayList<>();
            arg.add("101");
            arg.add("qwqwq");
            argList.add(arg);
            arg = new ArrayList<>();
            arg.add("102");
            arg.add("weqwe");
            argList.add(arg);
            String dst = "/apps/hive/warehouse/pokes";
            createFile(dst, argList);
            loadData2Hive(dst);

            select();
        }

        public static void select() {
            Connection connection = getConnection();
            String sql = "SELECT * FROM pokes";
            try {
                PreparedStatement preparedStatement = connection.prepareStatement(sql);
                ResultSet rs = preparedStatement.executeQuery();
                while (rs.next()) {
                    System.out.println(rs.getInt(1) + "\t" + rs.getString(2));
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }

        /**
         * 将数据上传到hdfs中，用于load到hive表中，设置分隔符是","
         */
        public static void createFile(String dst, List<List<String>> argList) {
            try (FileSystem fileSystem = FileSystem.get(new URI("hdfs://192.168.x.xx:8020"), new Configuration(), "hdfs");
                 FSDataOutputStream outputStream = fileSystem.create(new Path(dst))) {
                StringBuilder sb = new StringBuilder();
                for (List<String> arg : argList) {
                    for (String value : arg) {
                        // 分隔符要和建表时指定的分隔符相同，否则数据能插入，但是后续查询的结果为空
                        sb.append(value).append(",");
                    }
                    sb.deleteCharAt(sb.length() - 1);
                    sb.append("\n");
                }
                sb.deleteCharAt(sb.length() - 1);
                byte[] contents = sb.toString().getBytes();
                outputStream.write(contents);
                System.out.println("文件创建成功！");
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        /**
         * 将HDFS文件load到hive表中
         */
        public static void loadData2Hive(String dst) {
            String sql = " load data inpath '" + dst + "' into table pokes";
            try (Connection connection = getConnection(); PreparedStatement pstmt = connection.prepareStatement(sql)) {
                pstmt.execute();
                System.out.println("loadData到Hive表成功！");
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }

        public static Connection getConnection() {
            String driverName = "org.apache.hive.jdbc.HiveDriver";
            String connectionUrl = "jdbc:hive2://192.168.x.xx:10000/default";
            // 查询的时候不用用户名和密码也可以
            // 插入数据时必须要
            String username = "hdfs";
            String password = "hdfs";
            try {
                Class.forName(driverName);
            } catch (ClassNotFoundException e) {
                e.printStackTrace();
                System.exit(1);
            }
            Connection connection = null;
            try {
                connection = DriverManager.getConnection(connectionUrl, username, password);
            } catch (SQLException e) {
                e.printStackTrace();
            }
            return connection;
        }

    }
