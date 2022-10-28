package com.tools.parser.common;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

public class FileUtil {
    private String dirPath;

    public String getDirPath() {
        return dirPath;
    }

    public void setDirPath(String dirPath) {
        this.dirPath = dirPath;
    }

    //String fileName=filePath.substring(filePath.lastIndexOf("\\")+1);//文件名称
    //String name[]=fileName.split("\\.");
    List<File> fList=new ArrayList<File>();

    public List<File> getFiles(String dirPath) {

        boolean flag = true;
        File srcFile = new File(dirPath);

        if (!srcFile.isDirectory()) {
            flag=fList.add(srcFile);
            return fList;
        } else {
            File[] files = srcFile.listFiles();
            File[] var7 = files;
            int var6 = files.length;
            for(int var5 = 0; var5 < var6; ++var5) {
                File file = var7[var5];
                if (file.isFile()) {
                    flag = fList.add(file);
                } else if (file.isDirectory()) {
                    getFiles(file.getAbsolutePath());
                }

                if (!flag) {
                    break;
                }
            }
        }
        return fList;
    }
    /**
     * 读普通文件
     * @param file
     * @return string
     */
    public String fileReader(File file){
        FileInputStream fis = null;
        StringBuffer sb = new StringBuffer();
        try {
            fis = new FileInputStream(file);
            byte[] bytes = new byte[1024];
            while (-1 != fis.read(bytes)) {
                sb.append(new String(bytes));
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                fis.close();
            } catch (IOException e1) {
                e1.printStackTrace();
            }
        }
        return sb.toString();
    }
    /**
     * 读普通文件
     * @param filepath
     * @return string
     */
    public String fileReader(String filepath){
        FileInputStream fis = null;
        StringBuffer sb = new StringBuffer();
        try {
            fis = new FileInputStream(filepath);
            byte[] bytes = new byte[1024];
            while (-1 != fis.read(bytes)) {
                sb.append(new String(bytes));
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                fis.close();
            } catch (IOException e1) {
                e1.printStackTrace();
            }
        }
        return sb.toString();
    }
    /**
     * 读sql文件
     * @param file
     * @return string
     */
    public String sqlFileReader(File file){
        BufferedReader bf =null;
        StringBuffer sb = new StringBuffer();

        try {
            FileInputStream fr = new FileInputStream(file);
            InputStreamReader irs=new InputStreamReader(fr, StandardCharsets.UTF_8);
            bf = new BufferedReader(irs);
            String valueString = null;
            while ((valueString=bf.readLine())!=null){
                //System.out.println(valueString);
                //valueString=valueString.trim().substring(0,valueString.indexOf("--"));
                valueString=valueString.trim();
                if(valueString.length()>0 && valueString.lastIndexOf(";")+1==valueString.length()){
                    valueString=valueString+"/";
                }
                 if(valueString.toUpperCase().trim().indexOf("SET ")>=0) {
                     sb.append(" ").append("\n");//跳过set语句
                 }else{
                     sb.append(valueString+" ").append("\n");
                 }

            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                bf.close();
            } catch (IOException e1) {
                e1.printStackTrace();
            }
        }
        return sb.toString();
    }
    /**
     * 读普通文件
     * @param filepath
     * @return string
     */
    public static  void fileWriter(String filepath,String content,boolean bl) throws IOException {
        FileWriter fw = new FileWriter(filepath, bl);
        fw.write(content+"\n");
        fw.close();
    }
}
