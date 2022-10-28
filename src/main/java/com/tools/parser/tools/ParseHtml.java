package com.tools.parser.tools;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import com.tools.parser.common.FileUtil;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
public class ParseHtml {
    protected List<List<String>> data = new LinkedList<List<String>>();

    /**
     * 获取value值
     *
     * @param e
     * @return
     */
    public static String getValue(Element e) {
        return e.attr("value");
    }

    /**
     * 获取
     * <tr>
     * 和
     * </tr>
     * 之间的文本
     *
     * @param e
     * @return
     */
    public static String getText(Element e) {
        return e.text();
    }

    /**
     * 识别属性id的标签,一般一个html页面id唯一
     *
     * @param body
     * @param id
     * @return
     */
    public static Element getID(String body, String id) {
        Document doc = Jsoup.parse(body);
        // 所有#id的标签
        Elements elements = doc.select("#" + id);
        // 返回第一个
        return elements.first();
    }

    /**
     * 识别属性class的标签
     *
     * @param body
     * @param
     * @return
     */
    public static Elements getClassTag(String body, String classTag) {
        Document doc = Jsoup.parse(body);
        // 所有#id的标签
        return doc.select("." + classTag);
    }

    /**
     * 获取tr标签元素组
     *
     * @param e
     * @return
     */
    public static Elements getTR(Element e) {
        return e.getElementsByTag("tr");
    }

    /**
     * 获取td标签元素组
     *
     * @param e
     * @return
     */
    public static Elements getTD(Element e) {
        return e.getElementsByTag("td");
    }
    /**
     * 获取表元组
     * @param table
     * @return
     */
    public static List<ExcelBean> getTables(Element table){
        List<ExcelBean> data = new ArrayList<>();
        List<Element> lTr=table.select("tr");
        if (lTr.size()>0) {
            for (Element etr : lTr) {
                List<String> list = new ArrayList<>();
                ExcelBean eb = new ExcelBean();
                if(etr.select("td").size()>2) {
                    //增加一行中的一列
                    eb.setName(etr.select("td").get(0).text());
                    eb.setCode(etr.select("td").get(1).text());
                    eb.setType(etr.select("td").get(2).text());
                    //eb.setTooltype(etr.select("td").get(3).text());
                    //增加一行
                    data.add(eb);
                }
            }
        }
        return data;
    }
    public static void main(String[] args) {
        String filePath="D:\\逸迅科技\\workspace\\PJ_DW\\业务文档\\新CRM数据字典";

        String exFilePath="D:\\逸迅科技\\workspace\\parser\\output";
        String exFileName="新CRM字典";
        String exFileType="xls";
        ExcelWriter ew=new ExcelWriter();
        try {
            ExcelWriter.creater(exFilePath,exFileName,exFileType, new String[]{"表名","名称", "代码", "业务类型", "控件类型"});
        } catch (Exception e) {
            e.printStackTrace();
        }

        FileUtil fu=new FileUtil();
        List<File> files=fu.getFiles(filePath);
        for (File f: files) {

            Document doc = Jsoup.parse(fu.fileReader(f));
            // 获取table对象
            Element table = doc.select("table").first();
            // 获取title对象
            String title = doc.select("title").text();
            if (table!=null) {
                List<ExcelBean> list = getTables(table);
                list.remove(0);//去掉首行
                //添加表名
                for (int i = 0; i < list.size(); i++) {
                    list.get(i).setTitle(f.getName().replace(".html", ""));
                }
                try {
                    ew.dataWriter(list);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }
}
