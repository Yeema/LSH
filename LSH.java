package org.apache.hadoop.examples;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Arrays;
import java.io.StringReader;
import java.io.StreamTokenizer;
import java.util.StringTokenizer;
import java.util.Set;
import java.util.TreeSet;
import java.util.Iterator;
import java.util.HashMap;
import java.util.Map;
import java.util.HashSet;
import java.nio.file.Files;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import java.util.Vector;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapreduce.InputSplit;
import java.text.NumberFormat;
import java.text.DecimalFormat;
import java.util.Random;
import java.io.FileInputStream;
import java.io.InputStream;
import org.apache.hadoop.fs.FSDataInputStream;
import java.util.TreeMap;
public class LSH {
    public static NumberFormat NF = new DecimalFormat("00");
    static int k = 3;
	static int N = 12561;
    static int p = 12569;
    // static int numofarticle = 2;
    // static int minofarticle =100;
    static int iteration ;
    /*
        input 50 files
        output [three-shingling] [file no.]
     */
    public static class ShinglesMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        // String filename;

        // @Override
        // protected void setup(Context context) throws IOException, InterruptedException {
        //     FileSplit fsFileSplit = (FileSplit) context.getInputSplit();
        //     filename = context.getConfiguration().get(fsFileSplit.getPath().getParent().getName());
        // }
        //location holds file names
        private final static Text location = new Text();
        private String mapInputFileName;
        private int FileName;
        @Override
        protected void setup(Context context)
                throws IOException, InterruptedException {

            mapInputFileName = context.getConfiguration().get("map.input.file");

            FileSplit inputSplit = (FileSplit) context.getInputSplit();
            mapInputFileName = inputSplit.getPath().toString();
            mapInputFileName = mapInputFileName.substring("hdfs://nn/user/y32jjc00/yihuei/lshjava/data/".length(),mapInputFileName.length()-4);
            FileName = Integer.parseInt(mapInputFileName);
        }
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // FileSplit fileSplit = (FileSplit)context.getInputSplit();
            // String filename = fileSplit.getPath().getName();

            String line = value.toString().replaceAll("[^a-zA-Z]+", " ");
            StringTokenizer itr = new StringTokenizer(line.toLowerCase());
            ArrayList<String> collector = new ArrayList<String>();
            Text word = new Text();
            while (itr.hasMoreTokens()) {
                word.set(itr.nextToken());
                collector.add(word.toString());
                // context.write(new Text(mapInputFileName),new Text(word));
            }
            
            if(collector.size()>=k)
            {
                String fiveSH = "";
                for(int j=0 ; j<k ; j++){
                if( j != k-1)
                    fiveSH += collector.get(j)+"\t";
                else fiveSH += collector.get(j);
                }
                context.write(new Text(fiveSH) ,new IntWritable(FileName));
                for (int i = k; i < collector.size(); i++) {
                    int tabIndex = fiveSH.indexOf("\t");
                    fiveSH = fiveSH.substring(tabIndex+1);
                    fiveSH += "\t" + collector.get(i);
                    context.write(new Text(fiveSH),new IntWritable(FileName));
                }
            }
        }
    
	}
    /*
    input [three-shingling] [file no.]
    output [line no.] [[three-shingling] [file no1,file no2,file no3...]]
    0	a_list_of 10
    1	a_move_it 10
    2	a_new_outdoor 11
    3	a_new_uk 11
    4	a_return_to 10
    1 reducer
     */
	public static class ShinglesReducer extends Reducer<Text, IntWritable, IntWritable, Text> {
        private int counter = 0;
		@Override
		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			String files="";
            boolean first = true;
            for (IntWritable value : values){
                if(first){
                    files = Integer.toString(value.get());
                    first = false;
                }
                else
                    files += ","+Integer.toString(value.get()); 
            }
            context.write(new IntWritable(counter), new Text(String.join("_", key.toString().split("\\t"))+"\t"+files));
            counter += 1;
		}

	}
    /*
    input : <line no.> <three-shingling> <file no1>,<file no2>,<file no3>...
    output : <hashing no.> <file no1>,<file no2>,<file no3>...
     */
    public static class MinhashingMapper extends Mapper<LongWritable, Text, IntWritable, Text> {
        private int constanta;
        private int constantb;
        protected void setup(Context context)throws IOException, InterruptedException {
            constanta = (int)(Math.random()*10000);
            constantb = (int)(Math.random()*10000);
        }
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			int tIdx1 = value.find("\t");
            int number = Integer.parseInt( Text.decode(value.getBytes(), 0, tIdx1) );
            String[] splits = Text.decode(value.getBytes(), tIdx1 + 1, value.getLength() - (tIdx1 + 1)).split("\\t");
            String sharray = splits[1];
            // String shingling = splits[0];
            // String[] docArray = splits[1].split(",");
            int h = ((constanta*number + constantb)%p)%N;
            context.write(new IntWritable(h),new Text(sharray));
		}
    
	}
    /*
    input: <h> <file no1>,<file no2>,<file no3>...
    ouput: <iteration> <minhashingArr>
    1 reducer
     */
	public static class MinhashingReducer extends Reducer<IntWritable, Text, IntWritable, Text> {
        private int [] minhashingArr = new int [51];
        private int counter = 0;
        private int key_counter = 0;
        private boolean okay = true;
        @Override
        protected void setup(Context context)throws IOException, InterruptedException {
            Arrays.fill(minhashingArr, 0);
        }
		@Override
		public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			if(okay){
                for (Text value: values){
                    for( String val : value.toString().split(",")){
                        if ( minhashingArr[Integer.parseInt(val.toString())]==0){
                            minhashingArr[Integer.parseInt(val.toString())] = key.get();
                            counter+=1;
                        }
                        if (counter>49)
                            break;
                    }
                }
                key_counter+=1;
                if(key_counter==N || counter>49)
                {
                    String files="";
                    boolean first = true;
                    for (int i=1;i< minhashingArr.length;i++){
                        // myarray[value.get()-minofarticle] = 1;
                        if(first){
                            files = Integer.toString(minhashingArr[i]);
                            first = false;
                        }
                        else
                            files += ","+Integer.toString(minhashingArr[i]); 
                    }
                    context.write(new IntWritable(iteration),new Text(files));
                    okay = false;
                }
            } 
		}

	}
    /*
    input: <iteration> <minhashingArr>
    output: <iteration%50> <minhashingArr>
     */
    public static class MergeMapper extends Mapper<LongWritable, Text, Text, Text> {
        private final static Text location = new Text();
        private String mapInputFileName = "";
        static int number = 0;
        @Override
        protected void setup(Context context)
                throws IOException, InterruptedException {

            mapInputFileName = context.getConfiguration().get("map.input.file");

            FileSplit inputSplit = (FileSplit) context.getInputSplit();
            mapInputFileName = inputSplit.getPath().toString();
            mapInputFileName = mapInputFileName.substring("hdfs://nn/user/y32jjc00/yihuei/lshjava/out/minhashing/iter".length(),"hdfs://nn/user/y32jjc00/yihuei/lshjava/out/minhashing/iter".length()+2);
            number = Integer.parseInt(mapInputFileName);
        }
    
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			int tIdx1 = value.find("\t");
            // int number = (int)(Math.floor(((int)FileName/(int)4)));
            String minhashingStr = Text.decode(value.getBytes(), tIdx1 + 1, value.getLength() - (tIdx1 + 1)).toString();
            context.write( new Text(Integer.toString(number%50)) , new Text(minhashingStr));
		}
    
	}
    /*
    input: <iteration%50> <minhashingArr>
    ouput: <iteration%50> <minhashingArr pair>
    1 minhashingArr[0] minhashingArr[50]
    2 minhashingArr[1] minhashingArr[51]
     */
	public static class MergeReducer extends Reducer<Text, Text, Text, Text> {
		@Override
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String pairStr = "";
            boolean first = true;
            for (Text value: values){
                if (first)
                {
                    pairStr = value.toString();
                    first = false;
                }
                else
                    pairStr += "@@@"+value.toString();
            }
            context.write(key , new Text(pairStr));
	    }
    }
    /*
    input: <iteration%50> <minhashingArr pair>
    ouput: <pair[0],pair[1]> <strip no.> <article no.>
     */
    public static class LShashingMapper extends Mapper<LongWritable, Text, Text, Text> {
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			int tIdx1 = value.find("\t");
            int number = Integer.parseInt( Text.decode(value.getBytes(), 0, tIdx1) );
            String []spilts = Text.decode(value.getBytes(), tIdx1 + 1, value.getLength() - (tIdx1 + 1)).split("@@@");
            String []pair1 = spilts[0].split(",");
            String []pair2 = spilts[1].split(",");
            for(int i=0;i<pair1.length-1;i++){
                context.write(new Text(pair1[i]+","+pair2[i]),new Text(number+"\t"+Integer.toString(i+1)));
            }
		}
    
	}
    /*
    input: <pair[0],pair[1]> <strip no.> <article no.>
    ouput: <article no.> <article no.>
     */
	public static class LShashingReducer extends Reducer<Text, Text, IntWritable, IntWritable> {
		@Override
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Map<Integer,Set<Integer>> m1 = new HashMap<Integer,Set<Integer>>(); 
            
            for (Text value: values)
            {
                String []splits = value.toString().split("\\t");
                if(m1.containsKey(Integer.parseInt(splits[0]))){
                    m1.get(Integer.parseInt(splits[0])).add(Integer.parseInt(splits[1]));
                }else{
                    Set<Integer> a = new HashSet<Integer>();
                    a.add(Integer.parseInt(splits[1]));
                    m1.put(Integer.parseInt(splits[0]),a);
                }
            }
            for (Set<Integer> set : m1.values()) {
                boolean first = true;
                int a=-1;
                if(set.size()>1){
                    for (int val : set) {
                        if(first){
                            a = val;
                            first = false;
                        }else{
                            context.write(new IntWritable(a),new IntWritable(val));
                            a = val;
                        }
                    }
                }
            }   
        }	
	}
    /*
    input: <article no.> <article no.>
    ouput: <article no. smaller> <article no.larger>
     */
    public static class mergeLSHMapper extends Mapper<LongWritable, Text, IntWritable, IntWritable> {
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String []splits =value.toString().split("\\t");
			if (Integer.parseInt(splits[0]) < Integer.parseInt(splits[1]))
                context.write( new IntWritable(Integer.parseInt(splits[0])) , new IntWritable(Integer.parseInt(splits[1])));
            else if(Integer.parseInt(splits[1]) < Integer.parseInt(splits[0]))
                context.write( new IntWritable(Integer.parseInt(splits[1])) , new IntWritable(Integer.parseInt(splits[0])));

		}
    
	}
    /* 1 reducer
    input: <article no.> <article no.>
    ouput: <article no.> <article no.>
     */
	public static class mergeLSHReducer extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
		@Override
		public void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            Set<Integer> set=new HashSet<Integer>();  
            for (IntWritable value: values){
                set.add(value.get());
            }
            for(int val : set)
                context.write(key,new IntWritable(val));
	    }
    }
    public boolean job1(String in, String out) throws IOException, ClassNotFoundException, InterruptedException {
        
        Job job = Job.getInstance(new Configuration(), "Job #1");
        job.setJarByClass(LSH.class);
        
        // input / mapper
        FileInputFormat.addInputPath(job, new Path(in));
        job.setInputFormatClass(TextInputFormat.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setMapperClass(ShinglesMapper.class);
        // output / reducer
        FileOutputFormat.setOutputPath(job, new Path(out));
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setReducerClass(ShinglesReducer.class);
        job.setNumReduceTasks(1);
        
        return job.waitForCompletion(true);
     
    }
    public boolean job2(String in, String out) throws IOException, ClassNotFoundException, InterruptedException {
        
        Job job = Job.getInstance(new Configuration(), "Job #2");
        job.setJarByClass(LSH.class);
        
        // input / mapper
        FileInputFormat.addInputPath(job, new Path(in));
        job.setInputFormatClass(TextInputFormat.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setMapperClass(MinhashingMapper.class);
        // output / reducer
        FileOutputFormat.setOutputPath(job, new Path(out));
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);
        job.setReducerClass(MinhashingReducer.class);
        job.setNumReduceTasks(1);
        
        return job.waitForCompletion(true);
     
    }
    public boolean job3(String in, String out) throws IOException, ClassNotFoundException, InterruptedException {
        
        Job job = Job.getInstance(new Configuration(), "Job #3");
        job.setJarByClass(LSH.class);
        
        // input / mapper
        for (int i=0;i<100;i++ ){
            FileInputFormat.addInputPath(job, new Path(in+ NF.format(i)+"/p*"));   
        }
        job.setInputFormatClass(TextInputFormat.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setMapperClass(MergeMapper.class);
        // output / reducer
        FileOutputFormat.setOutputPath(job, new Path(out));
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setReducerClass(MergeReducer.class);
        return job.waitForCompletion(true);
     
    }
    public boolean job4(String in, String out) throws IOException, ClassNotFoundException, InterruptedException {
        
        Job job = Job.getInstance(new Configuration(), "Job #4");
        job.setJarByClass(LSH.class);
        
        // input / mapper
        FileInputFormat.addInputPath(job, new Path(in));
        job.setInputFormatClass(TextInputFormat.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setMapperClass(LShashingMapper.class);
        // output / reducer
        FileOutputFormat.setOutputPath(job, new Path(out));
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputKeyClass(LShashingReducer.class);
        job.setOutputValueClass(LShashingReducer.class);
        job.setReducerClass(LShashingReducer.class);
        return job.waitForCompletion(true);
     
    }
    public boolean job5(String in, String out) throws IOException, ClassNotFoundException, InterruptedException {
        
        Job job = Job.getInstance(new Configuration(), "Job #5");
        job.setJarByClass(LSH.class);
        
        // input / mapper
        FileInputFormat.addInputPath(job, new Path(in));
        job.setInputFormatClass(TextInputFormat.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setMapperClass(mergeLSHMapper.class);
        // output / reducer
        FileOutputFormat.setOutputPath(job, new Path(out));
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);
        job.setReducerClass(mergeLSHReducer.class);
        job.setNumReduceTasks(1);
        return job.waitForCompletion(true);
     
    }
    // public boolean job6(String in, String out) throws IOException, ClassNotFoundException, InterruptedException {
        
    //     Job job = Job.getInstance(new Configuration(), "Job #6");
    //     job.setJarByClass(LSH.class);
        
    //     // input / mapper
    //     FileInputFormat.addInputPath(job, new Path(in));
    //     job.setInputFormatClass(TextInputFormat.class);
    //     job.setMapOutputKeyClass(Text.class);
    //     job.setMapOutputValueClass(IntWritable.class);
    //     job.setMapperClass(pairsMapper.class);
    //     // output / reducer
    //     FileOutputFormat.setOutputPath(job, new Path(out));
    //     job.setOutputFormatClass(TextOutputFormat.class);
    //     job.setOutputKeyClass(Text.class);
    //     job.setOutputValueClass(DoubleWritable.class);
    //     job.setReducerClass(pairsReducer.class);
    //     job.setNumReduceTasks(1);
    //     return job.waitForCompletion(true);
     
    // }

	public static void main(String[] args) throws Exception{
        String input = "yihuei/lshjava/data";
        String outpath = "yihuei/lshjava/out";
        LSH lsh = new LSH();

        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);

        // input argu
        // String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		// if (otherArgs.length != 2) {
		// 	System.err.println("Usage: pagerank <in> <out>");
		// 	System.exit(2);
		// }

        boolean isCompleted = lsh.job1(input,outpath+"/shingling");
        // boolean isCompleted;
        if(!isCompleted)
			System.exit(1);
        iteration = 0;
        while (iteration<100 ){
            isCompleted = lsh.job2(outpath+"/shingling/p*",outpath+"/minhashing/iter"+ NF.format(iteration));
            if(!isCompleted)
			    System.exit(1);
            iteration+=1;
        }
        isCompleted = lsh.job3(outpath+"/minhashing/iter",outpath+"/merging");
        if(!isCompleted)
			System.exit(1);
        isCompleted = lsh.job4(outpath+"/merging/p*",outpath+"/lshashing");
        if(!isCompleted)
			System.exit(1);
        isCompleted = lsh.job5(outpath+"/lshashing/p*",outpath+"/mergelshashing");
        if(!isCompleted)
			System.exit(1);
        conf.addResource(new Path("/hadoop/projects/hadoop-1.0.4/conf/hdfs-site.xml"));
        String filePath = "yihuei/lshjava/out/mergelshashing/part-r-00000";

        Path path = new Path(filePath);
        FileSystem main_fs = path.getFileSystem(conf);
        FSDataInputStream inputStream = main_fs.open(path);
        String s;
        Vector<String> vec = new Vector<String>();
        while((s=inputStream.readLine())!=null){
            String [] pairs = s.split("\\t");
            vec.add(pairs[0]);
            vec.add(pairs[1]);
        }
        main_fs.close();

        filePath = "yihuei/lshjava/out/shingling/part-r-00000";
        path = new Path(filePath);
        main_fs = path.getFileSystem(conf);
        inputStream = main_fs.open(path);
        Vector<Vector<String>> shinglingVec = new Vector<Vector<String>>();
        while((s=inputStream.readLine())!=null){
            String [] splits = s.split("\\t");
            Vector<String> lineVec = new Vector<String>();
            String [] element = splits[2].split(",");
            for(int i=0;i<element.length;i++){
                lineVec.add(element[i]);
                // System.out.println(splits[i]);
            }
            shinglingVec.add(lineVec);
        }
        main_fs.close();

        TreeMap<Double,String> map = new TreeMap<Double,String>();
        for(int k=0;k<vec.size();k+=2){
            String a =vec.get(k);
            String b = vec.get(k+1);
            double one = 0.0;
            double two=0.0;
            for (int i = 0; i < shinglingVec.size(); i++){
                Vector inner = (Vector)shinglingVec.elementAt(i);
                boolean isa = inner.contains(a);
                boolean isb = inner.contains(b);
                if (isa && isb){
                    two+=1.0;
                }
                else if(isa || isb){
                    one+=1.0;
                }
            }
            // System.out.println(a+","+b+"\t"+two+" "+(one+two));
            map.put(two/(one+two) , a+","+b);
        }
        for(Map.Entry<Double,String> entry : map.descendingMap().entrySet()) {
            Double key = entry.getKey();
            String value = entry.getValue();

            System.out.println(value + " => " + key);
        }
    }
}