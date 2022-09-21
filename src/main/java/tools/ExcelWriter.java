package tools;

import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.hssf.util.HSSFColor;
import org.apache.poi.ss.usermodel.*;

import java.io.*;
import java.util.List;

import org.apache.poi.ss.util.CellRangeAddress;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

public class ExcelWriter {
    private static String excelPath;
    private static Workbook wb;
    private static Sheet sheet;
    private static Row row;

    public Workbook getWb() {
        return wb;
    }

    public void setWb(Workbook wb) {
        ExcelWriter.wb = wb;
    }

    public Sheet getSheet() {
        return sheet;
    }

    public void setSheet(Sheet sheet) {
        ExcelWriter.sheet = sheet;
    }

    public Row getRow() {
        return row;
    }

    public void setRow(Row row) {
        ExcelWriter.row = row;
    }

    public static void creater(String path, String fileName, String fileType, String[] titleRow) throws Exception {
        wb = null;
        excelPath = path+File.separator+fileName+"."+fileType;
        File file = new File(excelPath);
        sheet =null;
        //创建工作文档对象
        if (!file.exists()) {
            if (fileType.equals("xls")) {
                wb = new HSSFWorkbook();

            } else if(fileType.equals("xlsx")) {

                wb = new XSSFWorkbook();
            } else {
                throw new Exception("文件格式不正确");
            }
            //创建sheet对象
            sheet = wb.createSheet("sheet1");
            OutputStream outputStream = new FileOutputStream(excelPath);
            wb.write(outputStream);
            outputStream.flush();
            outputStream.close();

        } else {
            if (fileType.equals("xls")) {
                wb = new HSSFWorkbook();

            } else if(fileType.equals("xlsx")) {
                wb = new XSSFWorkbook();

            } else {
                throw new Exception("文件格式不正确");
            }
        }
        //创建sheet对象
        if (sheet==null) {
            sheet = wb.createSheet("sheet1");
        }

        //添加表头
        row = sheet.createRow(0);
        Cell cell = row.createCell(0);
        row.setHeight((short) 540);
        cell.setCellValue("新CRM表结构");    //创建第一行

        CellStyle style = wb.createCellStyle(); // 样式对象
        // 设置单元格的背景颜色为淡蓝色
        style.setFillForegroundColor(HSSFColor.PALE_BLUE.index);

        style.setVerticalAlignment(CellStyle.VERTICAL_CENTER);// 垂直
        style.setAlignment(CellStyle.ALIGN_CENTER);// 水平
        style.setWrapText(true);// 指定当单元格内容显示不下时自动换行

        cell.setCellStyle(style); // 样式，居中

        Font font = wb.createFont();
        font.setBoldweight(Font.BOLDWEIGHT_BOLD);
        font.setFontName("宋体");
        font.setFontHeight((short) 280);
        style.setFont(font);
        // 单元格合并
        // 四个参数分别是：起始行，起始列，结束行，结束列
        sheet.addMergedRegion(new CellRangeAddress(0, 0, 0, 7));
        sheet.autoSizeColumn(5200);

        row = sheet.createRow(1);    //创建第二行

        for(int i = 0;i < titleRow.length;i++){
            cell = row.createCell(i);
            cell.setCellValue(titleRow[i]);
            cell.setCellStyle(style); // 样式，居中
            sheet.setColumnWidth(i, 20 * 256);
        }
        row.setHeight((short) 540);


    }
    private static  Integer j=1;
    public void dataWriter( List<ExcelBean> list) throws IOException {
        //循环写入行数据
        for (int i = 0; i < list.size(); i++) {
            j=j+1;//增加新行，
            row = sheet.createRow(j);
            row.setHeight((short) 500);
            row.createCell(0).setCellValue(( list.get(i)).getTitle());
            row.createCell(1).setCellValue(( list.get(i)).getName());
            row.createCell(2).setCellValue(( list.get(i)).getCode());
            row.createCell(3).setCellValue(( list.get(i)).getType());
            row.createCell(4).setCellValue(( list.get(i)).getTooltype());
        }

        //创建文件流
        OutputStream stream = new FileOutputStream(excelPath);
        //写入数据
        wb.write(stream);
        //关闭文件流
        stream.close();
    }

}
