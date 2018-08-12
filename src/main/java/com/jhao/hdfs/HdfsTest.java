package com.jhao.hdfs;


import java.io.IOException;
import java.io.InputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
public class HdfsTest {
    
	FileSystem fs=null;  
		
   
    public static void createFile(String dst , byte[] contents) throws IOException{
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        Path dstPath = new Path(dst); 
        FSDataOutputStream outputStream = fs.create(dstPath);
        outputStream.write(contents);
        outputStream.close();
        fs.close();
        System.out.println("file created");
    }
    

    public static void uploadFile(String src,String dst) throws IOException{
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        Path srcPath = new Path(src); 
        Path dstPath = new Path(dst); 
        fs.copyFromLocalFile(false,srcPath, dstPath);
        System.out.println("Upload to "+conf.get("fs.default.name"));
        System.out.println("------------list files------------"+"\n");
        FileStatus [] fileStatus = fs.listStatus(dstPath);
        for (FileStatus file : fileStatus) 
        {
            System.out.println(file.getPath());
        }
        fs.close();
    }
    

    public static void rename(String oldName,String newName) throws IOException{
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        Path oldPath = new Path(oldName);
        Path newPath = new Path(newName);
        boolean isok = fs.rename(oldPath, newPath);
        if(isok){
            System.out.println("rename ok!");
        }else{
            System.out.println("rename failure");
        }
        fs.close();
    }

    public static void delete(String filePath) throws IOException{
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        Path path = new Path(filePath);
        boolean isok = fs.deleteOnExit(path);
        if(isok){
            System.out.println("delete ok!");
        }else{
            System.out.println("delete failure");
        }
        fs.close();
    }
    

    public static void mkdir(String path) throws IOException{
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        Path srcPath = new Path(path);
        boolean isok = fs.mkdirs(srcPath);
        if(isok){
            System.out.println("create dir ok!");
        }else{
            System.out.println("create dir failure");
        }
        fs.close();
    }
    

    public static void readFile(String filePath) throws IOException{
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        Path srcPath = new Path(filePath);
        InputStream in = null;
        try {
            in = fs.open(srcPath);
            IOUtils.copyBytes(in, System.out, 4096, false); 
        } finally {
            IOUtils.closeStream(in);
        }
    }
    
    
    public static void main(String[] args) throws IOException {
    	
    	mkdir("/test1");	
        uploadFile("/usr/local/storma.jar", "/test1");
        byte[] contents =  "hello world \n".getBytes();
        createFile("/test1/d.txt",contents);
        rename("/test1/d.txt", "/test1/dd.txt");
        readFile("/test1/dd.txt");
        delete("/test1/dd.txt"); 
        delete("/test1");    
        

    }

}
