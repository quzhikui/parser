package com.tools.parser.tools;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.tools.parser.common.FileUtil;
import io.trino.hadoop.$internal.org.apache.commons.io.FilenameUtils;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.List;

public class ParseJson {

    public static String parseJson(File file) throws Exception{
        //final File file = new File("C:\\Users\\屈志奎\\Desktop\\temp\\TMP_SRC.TS_NCRM_TBMXX.json");
        final BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(file)));
        String line;
        StringBuilder stringBuilder = new StringBuilder();
        while ((line = reader.readLine()) != null){
            stringBuilder.append(line);
        }
        JSONObject json = JSON.parseObject(String.valueOf(stringBuilder));
        JSONArray parse=json.getJSONObject("job").getJSONArray("content");
        JSONObject parse1=null;
        JSONArray parse2=null;
        String str =null;
        for (int i = 0; i < parse.size(); i++){
            parse1=parse.getJSONObject(i).getJSONObject("reader");
            for (int j = 0; j < parse1.size();j++){
                parse2=parse1.getJSONObject("parameter").getJSONArray("connection");
                if(parse2!=null) {
                    for (int k = 0; k < parse2.size(); k++) {
                        str = parse2.getJSONObject(k).getString("jdbcUrl");
                    }
                }else{
                    str="error";
                }
            }
        }
        return str;
    }
    public static void main(String[] args) throws Exception {
        String filePath="C:\\Users\\屈志奎\\Desktop\\temp\\整理\\";
        FileUtil fu=new FileUtil();
        List<File> files=fu.getFiles(filePath);
        System.out.println("***************"+"批量抽取json中的url"+"***************");
        for (File f: files) {
            String fileType = FilenameUtils.getExtension(f.getName()).toLowerCase();
            if (fileType.equals("json") ) {

                System.out.println("filename:"+f.getName()+"_URL:"+parseJson(f));

            }
        }
        System.out.println("***************结束***************");

    }
}
