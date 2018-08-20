package com.jhao.hdfs_example;

import java.io.File;
import java.io.IOException;
import java.util.Scanner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class ResultFilter {


	public static void main(String[] args) throws IOException{
		Configuration conf = new Configuration();
		//获取本地以及hadoop文件系统的类对象
		FileSystem hdfs = FileSystem.get(conf);
		FileSystem local = FileSystem.get(conf);
		Path inputDir,localFile;
		FileStatus[] inputFiles;//记录当前目录下有多少文件（包括文件夹）
		FSDataInputStream in = null;
		FSDataOutputStream out = null;
		Scanner scan;
		String str;
		byte[] buf;
		int singleFileLines;
		int numLines,numFiles,i;
		
		if(args.length!=4){
			System.out.println("参数不足四个!!");
			return;
		}
		
		//dfs path ：HDFS上的路径
		inputDir = new Path(args[0]);
		//single file lines ： 结果的每个文件的行数
		singleFileLines = Integer.parseInt(args[3]);
		
		
		try{
			
		inputFiles = hdfs.listStatus(inputDir);
		numLines = 0;
		numFiles = 1;//输出文件由1号开始编号
		//local path :服务器本地的路径
		localFile = new Path(args[1]);
		if(local.exists(localFile)){
			local.delete(localFile,true);
		}
		
		for(i=0;i<inputFiles.length;i++){
			//判断是不是目录
			//这里的isDir()已经是不推荐使用了，新API是什么？
			if(inputFiles[i].isDir()==true){
				//忽略子目录
				continue;
			}
			System.out.println(inputFiles[i].getPath().getName());
			in = hdfs.open(inputFiles[i].getPath());
			scan = new Scanner(in);
			while(scan.hasNext()){
				str = scan.nextLine();
				//开始match配对
				//arg【2】是要进行match的字符串
				if(str.indexOf(args[2])==-1){
					//这一行没有配对的字符串，忽略
					continue;
				}
				numLines++;
				if(numLines==1){
					//新建文件
					localFile = new Path(args[1]+File.separator+numFiles);
					out = local.create(localFile);
					numFiles++;//文件数++
				}
				//将match的那一行存入字节数组
				buf = (str+"\n").getBytes();
				out.write(buf,0,buf.length);
				
				if(numLines == singleFileLines){
					//如果一个文件里面的行数达到第四个命令arg[3]的要求时，关闭文件
					out.close();
					numLines = 0;//重新计数
				}
			}//while end
			//这个文件已经匹配完了
			scan.close();
			in.close();
		}//for end
		
		if(out !=null){
			out.close();
		}
		
	}catch(IOException e){
		e.printStackTrace();
	}
		
		
	}//main end
	
	
	
	
}
