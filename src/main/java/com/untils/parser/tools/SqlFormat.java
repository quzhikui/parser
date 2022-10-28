package com.untils.parser.tools;

import com.alibaba.druid.util.JdbcConstants;
import com.alibaba.druid.sql.SQLUtils;

public class SqlFormat {
    public static void main(String[] args) {
        //String mysql = "SELECT *,CASE WHEN UNIX_TIMESTAMP( expire_time ) < UNIX_TIMESTAMP( NOW( ) ) THEN 1 ELSE 0 END state FROM `expire_time_data`;";
        //mysql(mysql);
        //String oracle = "SELECT a.TABLE_NAME,b.COMMENTS FROM user_tables a,user_tab_comments b WHERE a.TABLE_NAME=b.TABLE_NAME ORDER BY TABLE_NAME;";
        //oracleSql(oracle);
        //String pgsql = "SELECT tablename FROM pg_tables WHERE tablename NOT LIKE 'pg%' AND tablename NOT LIKE 'sql_%' ORDER BY tablename;";
        //PgSql(pgsql);
        //sqlFormat(mysql, DbType.mysql);
        String hivesql="DROP TABLE IF EXISTS SD_ODS.TAG_IOU_INFO_ZIPPER;\n" +
                "CREATE TABLE IF NOT EXISTS SD_ODS.TAG_IOU_INFO_ZIPPER(\n" +
                "t_start_date        STRING        COMMENT '拉链起始日期',\n" +
                "t_end_date          STRING        COMMENT '拉链终止日期',\n" +
                "c_iou_no            STRING        COMMENT'借据编号-表主键',\n" +
                "c_nsbcont_code      STRING        COMMENT'非标合同编码',\n" +
                "c_loan_no           string        COMMENT'贷款协议号',\n" +
                "f_iou_amt           DECIMAL(16,2) COMMENT'借据金额',\n" +
                "f_iou_bal           DECIMAL(16,2) COMMENT'借据余额',\n" +
                "d_loan              TIMESTAMP     COMMENT'放款日期',\n" +
                "d_loan_due          TIMESTAMP     COMMENT'放款到期日',\n" +
                "l_roll_flag         BIGINT        COMMENT'展期标识',\n" +
                "c_order_sn          STRING        COMMENT'关联指令序列号',\n" +
                "c_source_code       STRING        COMMENT'采集源编码',\n" +
                "d_data              TIMESTAMP     COMMENT'数据日期'\n" +
                "\n" +
                ")COMMENT '借据信息拉链表'\n" +
                "ROW FORMAT DELIMITED\n" +
                "FIELDS TERMINATED BY ','\n" +
                "STORED AS ORC;";
        hiveSql(hivesql);

    }

    /**
     * mysql格式化
     * @param sql
     */
    public static void mysql(String sql){
        System.out.println("Mysql格式化：" + SQLUtils.formatMySql(sql));
    }

    /**
     * oracle格式化
     * @param sql
     */
    public static void oracleSql(String sql){
        System.out.println("Oracle格式化：" + SQLUtils.formatOracle(sql));
    }

    /**
     * pgsql格式化
     * @param sql
     */
    public static void PgSql(String sql){
        System.out.println("postgreSql格式化：" + SQLUtils.format(sql, JdbcConstants.POSTGRESQL));
    }
    /**
     * hivesql格式化
     * @param sql
     */
    public static void hiveSql(String sql){
        System.out.println("hiveSql格式化：\n" + SQLUtils.format(sql, JdbcConstants.HIVE));
    }

    /**
     * sql格式
     * @param sql 格式化的语句
     * @param dbType 数据库类型
     */
    public static void sqlFormat(String sql, String dbType){
        System.out.println("sql格式化：" + SQLUtils.format(sql, dbType));
    }
}
