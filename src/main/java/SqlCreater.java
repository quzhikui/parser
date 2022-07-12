import java.sql.*;

/*
* 连接mysql生成视图的ddl
* */
public class SqlCreater {
    public static void main(String[] args) throws SQLException, ClassNotFoundException {
        //Class.forName("com.mysql.jdbc.Driver");
        //Connection con= (Connection) DriverManager.getConnection("jdbc:mysql://192.168.32.136:3306/mydb","root","123456");
        main.java.Connections conns=new main.java.Connections();
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
