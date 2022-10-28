package com.tools.parser.tools;

import com.tools.parser.bean.Energy;
import com.tools.parser.bean.Links;
import com.tools.parser.bean.Nodes;
import com.tools.parser.common.FileUtil;
import com.tools.parser.common.JsonService;
import io.trino.hadoop.$internal.org.apache.commons.io.FilenameUtils;
import org.apache.hadoop.hive.ql.parse.ParseException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.tools.LineageInfo;

import java.io.File;
import java.util.*;
import java.util.regex.Pattern;

public class ParseSql4tab {
    private String insql;
    private Set<String> input_table;
    private Set<String> output_table;
    public ParseSql4tab() {
    }

    public ParseSql4tab(String sql, String input_table, String output_table) {
        this.insql = sql;
        this.input_table = Collections.singleton(input_table);
        this.output_table = Collections.singleton(output_table);
    }

    public String getInsql() {
        return insql;
    }

    public void setInsql(String insql) {
        this.insql = insql;
    }

    public Set<String> getInput_table() {
        return input_table;
    }

    public void setInput_table(Set<String> input_table) {
        this.input_table = input_table;
    }

    public Set<String> getOutput_table() {
        return output_table;
    }

    public void setOutput_table(Set<String> output_table) {
        this.output_table = output_table;
    }

    @Override
    public String toString() {
        return "ParseSql4tab{" +
                "insqlsql='" + insql + '\'' +
                ", input_table='" + input_table.toString() + '\'' +
                ", output_table='" + output_table.toString() + '\'' +
                '}';
    }
    public void run(){
        LineageInfo info=new LineageInfo();
        try {
            info.getLineageInfo(insql);
        } catch (SemanticException e) {
            e.printStackTrace();
        } catch (ParseException e) {
            e.printStackTrace();
        }

        setInput_table((Set<String>)info.getInputTableList());
        setOutput_table((Set<String>)info.getOutputTableList());
    }

    public static void main(String[] args) {
        String filePath="C:\\Users\\屈志奎\\Desktop\\temp\\finance\\";

        JsonService service = new JsonService();
        Energy eg = new Energy();

        FileUtil fu=new FileUtil();
        List<File> files=fu.getFiles(filePath);


        for (File f: files) {

            String fileType = FilenameUtils.getExtension(f.getName()).toLowerCase();
            Set<String> tables = new HashSet<String>();
            if (fileType.equals("sql")) {
                System.out.println("***************" + f.getName() + "***************");
                //对脚本文件名称进行处理
                String tab_name=null;
                if (f.getName().toUpperCase().startsWith("TFI_")) {
                    tab_name="SD_ODS."+f.getName().toUpperCase().substring(0,f.getName().lastIndexOf("."));
                }else{
                    tab_name="SD_EDW."+f.getName().toUpperCase().substring(0,f.getName().lastIndexOf("."));
                }
                //去除注释
                Pattern p = Pattern.compile("(?ms)('(?:''|[^'])*')|--.*?$|/\\*.*?\\*/|#.*?$|");
                String[] str = p.matcher(fu.sqlFileReader(f)).replaceAll("$1").split(";/");
                int val=1;
                Set<String> linksStr=new HashSet<String>();
                Set<String> nodesStr=new HashSet<String>();
                for (String parsesql : str) {
                    //System.out.println(parsesql);
                    //去掉行尾可能存在的分号”;“
                    if (parsesql.trim().endsWith(";")) {
                        parsesql = parsesql.trim();
                        parsesql = parsesql.substring(0, parsesql.length() - 1);
                    }
                    //去除空字符
                    if (parsesql.trim().length() > 0) {
                        ParseSql4tab ps4 = new ParseSql4tab();
                        ps4.setInsql(parsesql.trim());
                        ps4.run();
                        Set<String> input_table = ps4.getInput_table();
                        Set<String> output_table = ps4.getOutput_table();

                        if (input_table.size()!=0) {
                            for (String inptabstr:input_table){
                                if (inptabstr.contains(".")) {
                                     nodesStr.add(inptabstr.toUpperCase());
                                    if (output_table.size()!=0) {
                                        for (String ouptabstr:output_table){
                                            if (ouptabstr.contains(".")) {
                                                Nodes nd2 = new Nodes();
                                                nodesStr.add(ouptabstr.toUpperCase());
                                                if (!inptabstr.equalsIgnoreCase(ouptabstr) ) {
                                                    linksStr.add(inptabstr.toUpperCase()+":"+ouptabstr.toUpperCase());
                                                }
                                            }else{
                                                linksStr.add(inptabstr.toUpperCase()+":"+tab_name);
                                            }
                                        }
                                    }else{
                                        if (!inptabstr.equalsIgnoreCase(tab_name)) {
                                            linksStr.add(inptabstr.toUpperCase()+":"+tab_name);
                                        }
                                    }
                                    val+=1;
                                }
                            }
                        }

                    }
                }
                for (String ndstr : nodesStr) {
                    Nodes nd = new Nodes();
                    nd.setName(ndstr);
                    service.setNodesList(nd);
                }
                for (String lkstr : linksStr) {
                    Links lk = new Links();
                    String[]  lks=lkstr.split(":");
                    lk.setAllValue(lks[0],lks[1],val);
                    service.setLinksList(lk);
                }
            }

        }
        System.out.println(service.getEnergy().toString());
    }

}
