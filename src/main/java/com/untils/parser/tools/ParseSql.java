
package com.untils.parser.tools;

import com.untils.parser.common.ASTParse;
import com.untils.parser.common.FileUtil;
import io.trino.hadoop.$internal.org.apache.commons.io.FilenameUtils;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.ParseDriver;
import org.apache.hadoop.hive.ql.parse.ParseException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.tools.LineageInfo;

import java.io.File;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Pattern;

public class ParseSql{

    public  void run() throws ParseException, SQLException, ClassNotFoundException {
        //String filePath="C:\\Users\\Administrator\\Desktop\\temp\\1104\\";
        //String filePath="C:\\Users\\Administrator\\Desktop\\temp\\finance\\";
        String filePath="C:\\Users\\屈志奎\\Desktop\\temp\\1104\\";
        FileUtil fu=new FileUtil();
        List<File> files=fu.getFiles(filePath);
        for (File f: files) {
            String fileType = FilenameUtils.getExtension(f.getName()).toLowerCase();
            Set<String> tables = new HashSet<String>();
            if (fileType.equals("sql")) {
                System.out.println("***************" + f.getName() + "***************");
                //去除注释
                Pattern p = Pattern.compile("(?ms)('(?:''|[^'])*')|--.*?$|/\\*.*?\\*/|#.*?$|");
                String[] str = p.matcher(fu.sqlFileReader(f)).replaceAll("$1").split(";/");

                for (String parsesql : str) {

                    //去掉行尾可能存在的分号”;“
                    if (parsesql.trim().endsWith(";")) {
                        parsesql = parsesql.trim();
                        parsesql = parsesql.substring(0, parsesql.length() - 1);
                    }
                    //去除空字符
                    if (parsesql.trim().length() > 0) {
                        ParseDriver pd = new ParseDriver();
                        //调用hive解析器
                        ASTParse hp = new ASTParse();

                        //System.out.println(parsesql);
                        //将SQL解析为抽象语法树
                        try {
                            //处理一下分号没有去除的情况
                            ASTNode ast = pd.parse(parsesql);
                            //System.out.println(ast.toStringTree());
                            //hp.parseCol(ast);
                            tables.addAll(hp.parseTable(ast));
                        } catch (Exception e) {
                            System.out.println("出错SQL：" + parsesql);
                            e.printStackTrace();
                        }
                    }
                }
            }
            System.out.println(tables);
        }
    }
    public static void main(String[] args) throws ParseException, SQLException, ClassNotFoundException {
/*        ParseSql ps = new ParseSql();
        ps.run();*/

        String parsesql =
        "create temporary table table6\n" +
        "stored as orc\n" +
        "as\n" +
        "select\n" +
        "'2.逾期贷款' xiangmu,\n" +
        "cast(sum(if(qmye is null,0,qmye))/10000 as decimal(16,2)) bwbhj\n" +
        "from sd_ods.tfi_loan_detail where\n" +
        "--d_end < last_day(add_months('${hiveconf:DataDate}',-1));\n" +
        "yq<>'其他'\n"
;
        LineageInfo info=new LineageInfo();
        try {
            info.getLineageInfo(parsesql);
        } catch (SemanticException e) {
            e.printStackTrace();
        }
        TreeSet<String> its = info.getInputTableList();
        TreeSet<String> ots = info.getOutputTableList();
        System.out.println(its.toString());
        System.out.println(ots.toString());

/*
        String[] split = parsesql.split(";/");
       //去除空字符
        if (parsesql.trim().length() > 0) {
            ParseDriver pd = new ParseDriver();
            //调用hive解析器
            HiveParse hp = new HiveParse();

            //System.out.println(parsesql);
            //将SQL解析为抽象语法树
            try{
                ASTNode ast = pd.parse(parsesql);
                System.out.println(ast.toStringTree());
                hp.parseTable(ast);

                //hp.parseCol(ast);
            } catch(Throwable e){
                System.out.println("出错SQL："+parsesql);
                e.printStackTrace();
            }
        }*/

    }

}
