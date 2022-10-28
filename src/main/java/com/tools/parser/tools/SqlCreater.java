package com.tools.parser.tools;

import com.tools.parser.common.Connections;
import com.tools.parser.common.FileUtil;

import java.io.IOException;
import java.sql.*;
import java.util.Locale;


public class SqlCreater {
    public static void main(String[] args) throws SQLException, IOException, ClassNotFoundException {
        SqlCreater sc=new SqlCreater();
        sc.renewSrcCreater();
    }
    /*
     * 连接生产impala生成需要加密表的ddl
     * */
    public static void renewSrcCreater() throws IOException, SQLException, ClassNotFoundException {
        String sub_tab_str=null;
        String sqlstr=null;
        System.out.println("开始sql脚本生成.......\n");
        Connections conn= new Connections();
        Connection con=conn.getImpalaConnectionWithKRB();
        //Connection con=conn.getImpalaConnection();
        Statement stmt = con.createStatement();
        ResultSet rs = stmt.executeQuery("select distinct db_name,tab_name,sub_db_name from test.temp_quzk");
        while(rs.next()){//目标表
            String tab_str=rs.getString(1)+"."+rs.getString(2);
            sub_tab_str=rs.getString(3)+"."+rs.getString(2);
            //确认下游表是否存在
            Statement stmt3 = con.createStatement();

            try {
                ResultSet bl_rs = stmt3.executeQuery("select 1 from " + sub_tab_str);
                bl_rs.close();
            }catch(Exception e){
                //e.printStackTrace();
                System.out.println(sub_tab_str+"：不存在");
                continue;
            }finally {
                stmt3.close();
            }
            sqlstr = "INSERT OVERWRITE TABLE " + tab_str.toUpperCase(Locale.ROOT) + "\n";
            sqlstr = sqlstr + "SELECT" + "\n";

            String goal_col_str = "";
            Statement stmt2 = con.createStatement();
            ResultSet goal_col_rs = stmt2.executeQuery("select col_name from test.temp_quzk where tab_name='" + rs.getString(2) + "' and db_name='" + rs.getString(1) + "'");
            while (goal_col_rs.next()) {
                goal_col_str = goal_col_str + goal_col_rs.getString(1) + "|";
            }
            stmt2.close();

            Statement stmt1 = con.createStatement();
            ResultSet col_rs = stmt1.executeQuery("describe " + tab_str);
            while (col_rs.next()) {//重新查询表和字段
                String col_str = null;
                if (goal_col_str.contains(col_rs.getString(1))) {//确定加密字段
                    col_str = "'{encrypted}'||DES_ENCODER('" + col_rs.getString(1) + "'," + col_rs.getString(1) + "),\n";
                } else {
                    col_str = col_rs.getString(1) + ",\n";
                }
                sqlstr = sqlstr + col_str;
            }
            sqlstr = sqlstr.substring(0, sqlstr.length() - 2) + "\nFROM " + sub_tab_str.toUpperCase(Locale.ROOT) + ";";
            stmt1.close();
            FileUtil fu=new FileUtil();
            fu.fileWriter("script/"+sub_tab_str.toUpperCase(Locale.ROOT)+".hql",sqlstr.toUpperCase(Locale.ROOT),false);
            //fu.fileWriter(sub_tab_str.toUpperCase(Locale.ROOT)+".hql",sqlstr.toUpperCase(Locale.ROOT),false);
        }
        stmt.close();
        con.close();
        System.out.println("结束sql脚本生成.......\n");
    }
    /*
     * 连接mysql生成视图的ddl
     * */
    public static void viewCreater() throws SQLException, ClassNotFoundException {
        //Class.forName("com.mysql.jdbc.Driver");
        //Connection con= (Connection) DriverManager.getConnection("jdbc:mysql://192.168.32.136:3306/mydb","root","123456");
        Connections conns=new Connections();
        Connection con=conns.getmysqlConnection();


        //库名定义
        String dbna="SD_EDW";
        //表信息
        PreparedStatement tablePst=con.prepareStatement("select distinct tablename from mydb.quzk_table_col where dbname='"+dbna+"' and coltype is not null and coltype <>''");//where tablename='TS_NCRM_TRYXX'
        ResultSet tableRs=tablePst.executeQuery();

        while (tableRs.next()){
            String tab=tableRs.getString("tablename");
            //全量表字段
            PreparedStatement fullTablePst=con.prepareStatement("select distinct columnname,columncode,coltype from mydb.quzk_table_col  where upper(tablename)='"+tab+"'");
            ResultSet colRs=fullTablePst.executeQuery();
            //编写ddl
            String viewName=dbna+"."+tab+"_VIEW";
            //drop语句
            System.out.println("DROP VIEW IF EXISTS "+viewName+";");
            //create语句
            System.out.println("CREATE VIEW "+viewName+" (");
            while(colRs.next()){
                String col11=colRs.getString("columnname");
                String comments1=colRs.getString("columncode");
                if(!colRs.isLast()) {
                    System.out.println(col11 + " comment '" + comments1 + "',");
                }else{
                    System.out.println(col11 + " comment '" + comments1 + "'");
                }
            }
            System.out.println(") AS SELECT ");

            ResultSet colRs1=fullTablePst.executeQuery();
            while(colRs1.next()){
                String col1=colRs1.getString("columnname");
                String comments=colRs1.getString("columncode");
                String coltype=colRs1.getString("coltype");
                if(!colRs1.isLast()) {
                    if (coltype==null) {
                        System.out.println(col1 + ",--"+comments);
                    } else {
                            if (coltype.equals("电话") || coltype.equals("手机号") || coltype.equals("地址")) {
                                System.out.println("md5(" + col1 + ") as " + col1 + ",--"+comments);
                            } else if (coltype.equals("证件号")) {
                                System.out.println("substr("+col1+",1,6)|| md5(substr("+col1+",7)) as " + col1 + ",--"+comments);
                            }
                    }
                }else{
                    if (coltype==null) {
                        System.out.println(col1+"--"+comments );
                    } else {
                            if (coltype.equals("电话") || coltype.equals("手机号") || coltype.equals("地址")) {
                                System.out.println("md5(" + col1 + ") as " + col1+"--"+comments );
                            } else if (coltype.equals("证件号")) {
                                System.out.println("substr("+col1+",1,6)|| md5(substr("+col1+",7)) as " + col1 + "--"+comments);
                            }
                    }
                }
            }
            colRs.close();
            colRs1.close();
            fullTablePst.close();

            System.out.println("FROM "+dbna+"."+tab+";");
            System.out.println("------------------------------");
            System.out.println("                              ");
            System.out.println("------------------------------");
        }

        tableRs.close();
        tablePst.close();
        con.close();

    }
}
