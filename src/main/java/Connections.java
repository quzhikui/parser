package main.java;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class Connections {
    //测试环境自用mysql
    public Connection getmysqlConnection() throws ClassNotFoundException, SQLException {
        Class.forName("com.mysql.cj.jdbc.Driver");
        Connection con= DriverManager.getConnection("jdbc:mysql://192.168.32.136:3306/mydb","root","123456");
      return con;
    }
    //投研二期mysql
    public Connection getmysqlConnectionTY() throws ClassNotFoundException, SQLException {
        Class.forName("com.mysql.cj.jdbc.Driver");
        Connection con= DriverManager.getConnection("jdbc:mysql://192.168.32.27/iris-before","dbadmin","Ajxt@123");
        return con;
    }
    //投研二期mysql
    public Connection getHiveConnection() throws ClassNotFoundException, SQLException {
        Class.forName("com.cloudera.hive.jdbc4.HS1Driver");
        Connection con= DriverManager.getConnection("jdbc:hive2://192.168.32.132:10000/default");
        return con;
    }
}
