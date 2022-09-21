package common;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.IOException;
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
    //hive
    public Connection getHiveConnection() throws ClassNotFoundException, SQLException {
        //Class.forName("com.cloudera.hive.jdbc4.HS1Driver");
        Class.forName("org.apache.hive.jdbc.HiveDriver");
        Connection con= DriverManager.getConnection("jdbc:hive2://192.168.32.132:10000/default");
        return con;
    }
    //不带Kerberos认证的impala-jdbc连接
    public Connection getImpalaConnection() throws ClassNotFoundException, SQLException {
        Class.forName("com.cloudera.impala.jdbc.Driver");
        Connection con= DriverManager.getConnection("jdbc:impala://192.168.32.132:21050");
        return con;
    }
    //带Kerberos认证的impala-jdbc连接
    public static Connection getImpalaConnectionWithKRB() throws ClassNotFoundException, SQLException, IOException {
        // Kerberos
        final String KRB5_CONF = "/etc/krb5.conf";//linux中叫 krb5.conf,改后缀即可
        final String PRINCIPAL = "impala/ajxt-hdp-dn01@HADOOP.COM";
        final String KEYTAB = "/xdata/Multi-ExecuteFrame/conf/impala.keytab";
        // impala jdbc url 参数可参考官方文档   https://docs.cloudera.com/documentation/other/connectors/impala-jdbc/latest/Cloudera-JDBC-Driver-for-Impala-Install-Guide.pdf
        final String connectionUrl = "jdbc:impala://172.16.39.2:21050/;AuthMech=1;KrbRealm=HADOOP.COM;KrbHostFQDN=ajxt-hdp-dn01;KrbServiceName=impala";
        // 从官网下载的jar包
        final String jdbcDriverName = "com.cloudera.impala.jdbc.Driver";

        System.setProperty("java.security.krb5.conf", KRB5_CONF);
        Configuration conf = new Configuration();
        conf.set("hadoop.security.authentication", "Kerberos");
        UserGroupInformation.setConfiguration(conf);
        UserGroupInformation.loginUserFromKeytab(PRINCIPAL, KEYTAB);
        System.out.println(">> 1. Login from keytab " + KEYTAB + " Success");
        UserGroupInformation loginUser = UserGroupInformation.getLoginUser();
        //加载驱动
        try {
            Class.forName(jdbcDriverName);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        System.out.println(">> 2. jdbcDriver load Success");
        Connection con = null;
        try {
            con = DriverManager.getConnection(connectionUrl);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return con;
    }
}
