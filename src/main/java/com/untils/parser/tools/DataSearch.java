package com.untils.parser.tools;

import com.untils.parser.common.Connections;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;

/**
 * @Date: 2022/1/5
 * @Author: BigBig-Data
 * @Description: 注： impala的JDBC驱动maven远程仓库中没有, 需要从别的地方下载： https://www.cloudera.com/search.html?q=impala%20jdbc
 */
public class DataSearch {

    public static void main(String[] args) throws Exception {
        String[] db_name_list = new String[]{"sd_ifd", "sd_src"};
        FileWriter fw = null;
        String filePath = "datasearch_" + ".txt";
        File file = new File(filePath);  //先New出一个文件来
        FileOutputStream fos = new FileOutputStream(file); //然后再New出一个文件输出流，
        if (!file.exists()) {
            file.createNewFile();
        }

        fw = new FileWriter(filePath);
        BufferedWriter bw = new BufferedWriter(fw);

        DataSearch ds = new DataSearch();
        Connections conn = new Connections();
        Connection con = conn.getImpalaConnectionWithKRB();
        //Connection con=conn.getImpalaConnection();
        Statement stmt = con.createStatement();

        fos.write("开始查找数据.............\n".getBytes());

        for (String db_name : db_name_list) {

            boolean fs = stmt.execute("use " + db_name);
            Statement stmt1 = con.createStatement();
            ResultSet tab_rs = stmt1.executeQuery("show tables");
            int i = 0;

            while (tab_rs.next()) {
                //System.out.println(tab_rs.getString(1));
                Statement stmt2 = con.createStatement();
                try {
                    ResultSet col_rs = stmt2.executeQuery("describe " + db_name + "." + tab_rs.getString(1));
                    while (col_rs.next()) {
                        //1，查找身份证号
                        if ((tab_rs.getString(1).length() > 5)
                                && col_rs.getString(2).equals("string")
                                && !tab_rs.getString(1).substring(tab_rs.getString(1).length() - 5).equals("_view")
                                && !tab_rs.getString(1).contains("ts_jy_")
                                && !tab_rs.getString(1).contains("ts_zy_")) {
                            //System.out.println(col_rs.getString(1));
                            String sqlstr = "select 1 from " + db_name + "." + tab_rs.getString(1) +
                                    " where `" + col_rs.getString(1) + "` regexp '^[1-9]\\\\d{5}(18|19|20|(3\\\\d))\\\\d{2}((0[1-9])|(1[0-2]))(([0-2][1-9])|10|20|30|31)\\\\d{3}[0-9Xx]$' "
                                    + "limit 1";
                            Statement stmt5 = con.createStatement();
                            try {

                                ResultSet data_rs = stmt5.executeQuery(sqlstr);
                                if (data_rs.isBeforeFirst()) {
                                    while (data_rs.next()) {
                                        if (data_rs.getInt(1) >= 1) {
                                            i++;
                                            System.out.println(sqlstr);
                                            String str = "序号:" + i + ".身份证号：" + db_name +"."+tab_rs.getString(1) + "." + col_rs.getString(1) + ".字段备注：" + col_rs.getString(3) + "\n";
                                            fos.write(str.getBytes());
                                        }
                                    }
                                }
                            } catch (Exception e) {
                                e.printStackTrace();
                            } finally {
                                stmt5.close();
                            }
                        }
                        //2，查找地址,电话，手机号字段
                        //System.out.println(col_rs.getString(2));
                    /*
                    if ((tab_rs.getString(1).length() > 5)
                            && col_rs.getString(2).equals("string")
                            && !tab_rs.getString(1).substring(tab_rs.getString(1).length() - 5).equals("_view")
                            && !tab_rs.getString(1).contains("ts_jy_")
                            && !tab_rs.getString(1).contains("ts_zy_")) {

                        //System.out.println(col_rs.getString(1));
                        String sqlstr = "select 1 from " + db_name + "." + tab_rs.getString(1) +
                                " where (`" + col_rs.getString(1) + "`  like '%省%'   or  `"
                                + col_rs.getString(1) + "`  like '%市%'   or  `"
                                + col_rs.getString(1) + "`  like '%县%'   or  `"
                                + col_rs.getString(1) + "`  like '%乡%'   or  `"
                                + col_rs.getString(1) + "`  like '%区%'   or  `"
                                + col_rs.getString(1) + "`  like '%街道%' or  `"
                                + col_rs.getString(1) + "`  like '%居委%' or  `"
                                + col_rs.getString(1) + "`  like '%村%'   or  `"
                                + col_rs.getString(1) + "`  like '%室%'   or  `"
                                + col_rs.getString(1) + "` regexp '\\\\d{3}-\\\\d{8}|\\\\d{4}-\\\\d{7}' or `"
                                + col_rs.getString(1) + "` regexp '(13[0-9]|14[5|7]|15[0|1|2|3|5|6|7|8|9]|18[0|1|2|3|5|6|7|8|9])\\\\d{8}$' "
                                + ") and (`"
                                + col_rs.getString(1) + "` not like '%信托计划%'  and `"
                                + col_rs.getString(1) + "` not like '%信托'     and `"
                                + col_rs.getString(1) + "` not like '%有限公司%' and `"
                                + col_rs.getString(1) + "` not like '%有限责任公司%' and `"
                                + col_rs.getString(1) + "` not like '%（次级）' and `"
                                + col_rs.getString(1) + "` not like '%（优先）' and `"
                                + col_rs.getString(1) + "` not like '%（优先）' and `"
                                + col_rs.getString(1) + "` not like '%贷款%' and `"
                                + col_rs.getString(1) + "` not like '%投资' and `"
                                + col_rs.getString(1) + "` not like '%支行' and `"
                                + col_rs.getString(1) + "` not like '%合同' and `"
                                + col_rs.getString(1) + "` not like '%市场%' and `"
                                + col_rs.getString(1) + "` not like '%专户%' "
                                + ") and `" + col_rs.getString(1) + "`  is not null "
                                + "limit 1";
                        Statement stmt3 = con.createStatement();
                        try {
                            System.out.println(sqlstr);
                            ResultSet data_rs = stmt3.executeQuery(sqlstr);
                            if (data_rs.isBeforeFirst()) {
                                while (data_rs.next()) {
                                    if (data_rs.getInt(1) >= 1) {
                                        i++;
                                        //System.out.println("序号:"+i+".地址,电话，手机号字段："+tab_rs.getString(1)+"."+col_rs.getString(1)+".字段备注："+col_rs.getString(2));
                                        String str = "序号:" + i + ".地址,电话，手机号字段：" + tab_rs.getString(1) + "." + col_rs.getString(1) + ".字段备注：" + col_rs.getString(3) + "\n";
                                        fos.write(str.getBytes());
                                    }
                                }
                            }
                        } catch (Exception e) {
                            e.printStackTrace();
                        } finally {
                            stmt3.close();
                        }

                    }
                    else if ((tab_rs.getString(1).length() > 5)
                            && (col_rs.getString(2).equals("bigint") || col_rs.getString(2).equals("int"))
                            && !tab_rs.getString(1).substring(tab_rs.getString(1).length() - 5).equals("_view")
                            && !tab_rs.getString(1).contains("ts_jy_")
                            && !tab_rs.getString(1).contains("ts_zy_")) {
                        String sqlstr = "select 1 from " + db_name + "." + tab_rs.getString(1) +
                                " where (cast(`" + col_rs.getString(1) + "` as string) regexp '\\\\d{3}-\\\\d{8}|\\\\d{4}-\\\\d{7}' " +
                                "or cast(`" + col_rs.getString(1) + "`  as string) regexp '(13[0-9]|14[5|7]|15[0|1|2|3|5|6|7|8|9]|18[0|1|2|3|5|6|7|8|9])\\\\d{8}$' "
                                + ") and cast(`" + col_rs.getString(1) + "`  as string) is not null "
                                + "limit 1";
                        Statement stmt4 = con.createStatement();
                        try {
                            System.out.println(sqlstr);
                            ResultSet data_rs = stmt4.executeQuery(sqlstr);
                            if (data_rs.isBeforeFirst()) {
                                while (data_rs.next()) {
                                    if (data_rs.getInt(1) >= 1) {
                                        i++;
                                        //System.out.println("序号:"+i+".地址,电话，手机号字段："+tab_rs.getString(1)+"."+col_rs.getString(1)+".字段备注："+col_rs.getString(2));
                                        String str = "序号:" + i + ".电话，手机号字段：" + tab_rs.getString(1) + "." + col_rs.getString(1) + ".字段备注：" + col_rs.getString(3) + "\n";
                                        fos.write(str.getBytes());
                                    }
                                }
                            }
                        } catch (Exception e) {
                            e.printStackTrace();
                        } finally {
                            stmt4.close();
                        }
                    }*/
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                } finally {
                    stmt2.close();
                }
            }
            stmt1.close();
        }
        fos.write("结束查找数据.............\n".getBytes());
        stmt.close();
        con.close();
        fos.flush();                                      //flush输出流
        fos.close();                                      //close输出流

    }

}

