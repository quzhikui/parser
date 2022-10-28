package com.tools.parser.tools;

import com.tools.parser.common.Connections;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.*;
/*
 * 生成datax的json脚本
 *
 *
 */
public class JsonCreater {
    //hive2mysql字段转换器
    private static String hive2mysqlColConverter(String colType){
        if(colType.equals("int")||colType.equals("bigint")||colType.equals("smallint")){
            return "long";
        }else if(colType.equals("datetime")||colType.equals("timestamp")){
            return "date";
        }else if(colType.contains("decimal")||colType.contains("numeric")){
            return "double";
        }else if(colType.contains("char")){
            return "string";
        }
        else{ return "string";}

    }
    /*
     * 生成hive2mysql生成datax的json脚本
     * */
    public static String jsonCreater(String reader,String writer,String srcBasePath,String srcDb,String srcTablename,String desTablename) throws SQLException, ClassNotFoundException {
        String jsonStr="\n            {\n" +
                "                \"job\": {";
        //编写setting
        jsonStr=jsonStr+"\n                    \"setting\": {\n" +
                "                        \"speed\": {\n" +
                "                            \"channel\": 1\n" +
                "                        }\n" +
                "                    },";
        //编写content
        jsonStr=jsonStr+"\n                    \"content\": [{";
        if(reader.equals("HDFS")){
            jsonStr=jsonStr+hdfsReaderCreater(srcBasePath,srcDb,srcTablename);
        }

        jsonStr=jsonStr+("\n,");
        if(writer.equals("MYSQL")){
            jsonStr=jsonStr+mysqlWriterCreater(desTablename);
        }

        jsonStr=jsonStr+("\n                    }]");
        jsonStr=jsonStr+("\n                }\n" +
                "            }");
        return jsonStr;
    }
    //HDFSReader

    public static String hdfsReaderCreater(String basePath,String db,String tablename) throws SQLException, ClassNotFoundException {
        Connections conns=new Connections();
        Connection con2=conns.getHiveConnection();

        String path= basePath+db+".db/"+tablename+"/*";
        String defaultFS="hdfs://nnhaservice";
        String fileName=db+"."+tablename;

        String jsonStr="\n                \"reader\": {\n" +
                "                    \"name\": \"hdfsreader\",\n" +
                "                    \"parameter\": {\n" +
                "                        \"path\": \""+path+"\",";
        jsonStr=jsonStr+"\n                        \"defaultFS\": \""+defaultFS+"\",";
        jsonStr=jsonStr+"\n                        \"fileName\": \""+fileName+"\",";
        jsonStr=jsonStr+"\n                        \"column\": [";

        //处理字段
        //表信息
        PreparedStatement tablePst=con2.prepareStatement("desc "+fileName);
        ResultSet hiveTableRs=tablePst.executeQuery();
        Integer i=0;
        while (hiveTableRs.next()) {
            String colType = hiveTableRs.getString("data_type");
            if(!hiveTableRs.isLast()) {
                jsonStr=jsonStr+"\n                            {\"index\": "+i+",\"type\": \""+hive2mysqlColConverter(colType)+"\"},";
            }else{
                jsonStr=jsonStr+"\n                            {\"index\": "+i+",\"type\": \""+hive2mysqlColConverter(colType)+"\"}";
            }
            i++;
        }
        //特殊处理-hive
        jsonStr=jsonStr.substring(0,jsonStr.length()-1);

        jsonStr=jsonStr+"\n                        ],                    \n" +
                "                        \"fileType\": \"orc\",\n" +
                "                        \"encoding\": \"UTF-8\",\n" +
                "                        \"hadoopConfig\":{\n" +
                "                                \"dfs.nameservices\": \"nnhaservice\",\n" +
                "                                \"dfs.ha.namenodes.nnhaservice\": \"namenode144,namenode390\",\n" +
                "                                \"dfs.namenode.rpc-address.nnhaservice.namenode144\": \"ajxt-hdp-dn01:8020\",\n" +
                "                                \"dfs.namenode.rpc-address.nnhaservice.namenode390\": \"ajxt-hdp-dn02:8020\",\n" +
                "                                \"dfs.client.failover.proxy.provider.nnhaservice\": \"org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider\"\n" +
                "                        },\t\n";

        //Kerberos认证
        jsonStr=jsonStr+
                "                        \"haveKerberos\": true,\n" +
                "                        \"kerberosKeytabFilePath\": \"/xdata/Multi-ExecuteFrame/conf/hdfs.keytab\",\n" +
                "                        \"kerberosPrincipal\": \"hdfs/ajxt-hdp-dn01@HADOOP.COM\",\t\t\t\t\t\t\n" ;

        jsonStr=jsonStr+"                        \"fieldDelimiter\": \",\"\n" +
        "                    }\n" +
                "\n" +
                "                }";
        return jsonStr;
    }
    //mysqlWriter
    public static String mysqlWriterCreater(String tablename) throws SQLException, ClassNotFoundException {
        Connections conns=new Connections();
        Connection con1=conns.getmysqlConnectionTY();

        String acct="ajxtiris";//con1.getMetaData().getUserName();
        String passwd="Ajxtiris@123!~";//-con1.getClientInfo().getProperty("password");
        String url="jdbc:mysql://172.16.33.81:3306/iris-before?useUnicode=true&characterEncoding=UTF-8";//con1.getMetaData().getURL();
        String schema="iris-before";//con1.getSchema();

        PreparedStatement tablePst=con1.prepareStatement("SELECT column_name FROM information_schema.columns " +
                "WHERE table_schema='"+schema+"' AND table_name='"+tablename+"'; ");
        ResultSet mysqlTableRs=tablePst.executeQuery();
        Integer i=0;

        String jsonStr="\n                        \"writer\": {\n" +
                "                            \"name\": \"mysqlwriter\",\n" +
                "                            \"parameter\": {\n" +
                "                                \"writeMode\": \"insert\",\n" +
                "                                \"username\": \""+acct+"\",\n" +
                "                                \"password\": \""+passwd+"\",\n" +
                "                                \"column\": [\n";


        while (mysqlTableRs.next()) {
            String column_name = mysqlTableRs.getString("column_name");
            if(!mysqlTableRs.isLast()) {
                jsonStr=jsonStr+"\n\t\t\t\t\t\t\t\t\t\t\""+column_name+"\",\n";
            }else{
                jsonStr=jsonStr+"\n\t\t\t\t\t\t\t\t\t\t\""+column_name+"\"\n";
            }
            i++;
        }

        jsonStr=jsonStr+"\n                                ],\n" +
                "                                \"session\": [\n" +
                "                                    \"set session sql_mode='ANSI'\"\n" +
                "                                ],\n" +
                "                                \"preSql\":[\"truncate table "+tablename+"\"],\n" +
                "                                \"connection\": [{\n" +
                "                                    \"jdbcUrl\": \""+url+"\",\n" +
                "                                    \"table\": [\n" +
                "                                        \""+tablename+"\"\n" +
                "                                    ]\n" +
                "                                }]\n" +
                "                            }\n" +
                "                        }";
        return jsonStr;

    }

        public static void main(String[] args) throws SQLException, ClassNotFoundException, IOException {
            String jsonStr=null;
           String[] mysqlTable= {
                   "dw_fund_info",
                   "dw_fund_net_value",
                   "dw_bench_mark_info",
                   "dw_bench_mark_close_price",
                   "dw_trading_day",
                   "dw_ct_system_const",
                   "dw_tcd_dict_key",
                   "dw_invest_advisor_list",
                   "dw_system_label_pool",
                   "dw_system_label_mapping"
           };
            String[] HiveTable={
                    "sd_edw.mty_tq_subfund_d",
                    "sd_edw.mty_tq_subfund_netvalue_d",
                    "sd_edw.mty_standard_d",
                    "sd_edw.mty_standard_netvalue_d",
                    "sd_src.ts_jy_qt_tradingdaynew",
                    "sd_src.ts_jy_ct_systemconst",
                    "sd_ods.tcd_dict_key",
                    "sd_edw.mty_tq_invest_advisor_list_d",
                    "sd_edw.mty_tq_system_label_pool_d",
                    "sd_edw.mty_tq_system_label_mapping_d"
            };
            for (int i = 0; i < mysqlTable.length; i++) {
                    String[] str=HiveTable[i].split("[.]");
                    String srcDb=str[0];
                    String srcTableName=str[1];
                    jsonStr=jsonCreater("HDFS","MYSQL","/user/hive/warehouse/",srcDb,srcTableName,mysqlTable[i]);
                    //System.out.println(jsonStr);

                    //写入文件
                    File file =new File("D:\\逸迅工作\\workspace\\投研数据接口\\同步脚本\\投前\\"+mysqlTable[i]+".json");
                    if(!file.exists()){
                        file.createNewFile();
                    }
                FileWriter fw = new FileWriter(file.getAbsoluteFile());
                BufferedWriter bw = new BufferedWriter(fw);
                bw.write(jsonStr);
                bw.close();
                //bw.flush();
                bw.close();
                    System.out.println(mysqlTable[i]+" Done......");
            }

        }
}
