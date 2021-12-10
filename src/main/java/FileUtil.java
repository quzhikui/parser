import java.io.File;
import java.util.ArrayList;
import java.util.List;

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
}
