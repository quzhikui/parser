/*
package tools;

import common.FileUtil;
import io.trino.hadoop.$internal.org.apache.commons.io.FilenameUtils;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.ParseDriver;
import org.apache.hadoop.hive.ql.parse.ParseException;

import java.io.File;
import java.sql.SQLException;
import java.util.List;

public class ParseSql{
    public static void main(String[] args) throws ParseException, SQLException, ClassNotFoundException {
        //String filePath="C:\\Users\\Administrator\\Desktop\\temp\\1104\\";
        //String filePath="C:\\Users\\Administrator\\Desktop\\temp\\finance\\";
        String filePath="C:\\Users\\屈志奎\\Desktop\\temp\\整理\\sub\\";
        FileUtil fu=new FileUtil();
        List<File> files=fu.getFiles(filePath);
        for (File f: files) {
            String fileType = FilenameUtils.getExtension(f.getName()).toLowerCase();
            if (fileType.equals("sql") ) {
                System.out.println("***************"+f.getName()+"***************");
                String[] str = fu.sqlFileReader(f).split(";/");
               for (String parsesql : str) {
                    //去除空字符
                    if (parsesql.trim().length() > 0) {
                        ParseDriver pd = new ParseDriver();
                        //调用hive解析器
                        HiveParse hp = new HiveParse();

                        //System.out.println(parsesql);
                        //将SQL解析为抽象语法树
                        try{
                            ASTNode ast = pd.parse(parsesql);
                            //System.out.println(ast.toStringTree());
                            hp.parseTable(ast);

                            //hp.parseCol(ast);
                        } catch(Throwable e){
                            //System.out.println("出错SQL："+parsesql);
                            //e.printStackTrace();
                        }
                    }
                }
                System.out.println("***************结束***************");
            }
        }
    }
}*/
