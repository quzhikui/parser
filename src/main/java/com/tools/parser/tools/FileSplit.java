package com.tools.parser.tools;

import java.io.*;
import java.util.Locale;

public class FileSplit {
    /**
     * 读普通文件
     * @param filepath
     * @return string
     */
    public static String fileReader(String filepath){
        InputStreamReader isr = null;
        BufferedReader br = null;
        StringBuffer sb = new StringBuffer();
        try {
             isr= new InputStreamReader(new FileInputStream(filepath),"UTF-8");
             br=new BufferedReader(isr);

            String line;
            String str = null;
            while ((line= br.readLine())!=null) {

                if (line.contains("DROP TABLE IF EXISTS")){

                    str=line.substring(line.indexOf(".")+1,line.lastIndexOf(";")).toLowerCase(Locale.ROOT);
                    System.out.print(str+"\n");
                    sb.append(line+"\n");
                } else {
                    if(line.contains("STORED AS ORC;")){
                        sb.append(line+"\n");
                        File f=new File(str+".sql");
                        if(f.createNewFile())
                        {
                            System.out.print(str+"文件已生产\n");
                        }
                        try{
                            FileWriter fw=new FileWriter(f);
                            fw.write(sb.toString());
                            fw.close();

                        }catch(Exception e){
                            e.printStackTrace();
                        }

                        sb.setLength(0);
                    }else{
                        sb.append(line+"\n");
                    }
                }
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                isr.close();
                br.close();
            } catch (IOException e1) {
                e1.printStackTrace();
            }
        }
        return sb.toString();
    }
    public static void main(String[] args) {
        String filePath="D:\\逸迅科技\\爱建信托\\爱建信托数仓模型设计\\数仓自有表备份\\数仓自有表备份.sql";
        fileReader(filePath);
        //System.out.print(fileReader(filePath));
        System.out.print("\n--------------------------------------");
    }
}
