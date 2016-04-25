import java.io.IOException;
import java.util.*;
        
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
        
public class PhaseThree 
{
    public static class Map extends Mapper<LongWritable, Text, Text, Text>
    {
	
	private Text outKey = new Text();
        
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException 
	{
	    String inputLine = value.toString(); //input is coming from the output file from phase one
	    String temp[] = inputLine.split("\t"); //spliting input string to get pair of word,document name and frequency
	    
	    String docPart[]=temp[0].split(",");//seperating document name and word
	    String CalPart[]=temp[1].split(",");
	    
	    String word = docPart[0];//getting the input word
	    outKey.set(word);
      context.write(outKey, new Text(docPart[1]+","+CalPart[0]+","+CalPart[1]+","+"1"));
	    
	    
	}
    } 
        
    public static class Reduce extends Reducer<Text, Text, Text, Text>
    {

   
	public void reduce(Text key, Iterable<Text> values, Context context) 
	    throws IOException, InterruptedException
	    {
    ArrayList<String> docnames=new ArrayList<String>();
    ArrayList<String> n=new ArrayList<String>();
    ArrayList<String> N=new ArrayList<String>();
    double m = 0;

          for (Text val : values) {
	        	
	          String vals[]=val.toString().split(",");
	          docnames.add(vals[0]);
	          n.add(vals[1]);
	        	N.add(vals[2]);
	        	m += Double.parseDouble(vals[3]);
	        }
    
	    for(int i=0;i<docnames.size();i++) {
				
				double nValue= Double.parseDouble(n.get(i));
				double NValue= Double.parseDouble(N.get(i));

				double tf=nValue/NValue;
				
				double idf= Math.log(12.0/m)/ Math.log(2);//Had to hardcode the number of documents
        double tfIdf = tf * idf;			
				context.write(new Text(key+","+docnames.get(i)), new Text(""+tfIdf)); 
			}
	    
	    }
    }
        
    public static void main(String[] args) throws Exception
    {
	Configuration conf = new Configuration();
        
        Job job = new Job(conf, "PhaseThree");
    
	job.setOutputKeyClass(Text.class);
	job.setOutputValueClass(Text.class);
	job.setJarByClass(PhaseThree.class);

	job.setMapperClass(Map.class);
	job.setReducerClass(Reduce.class);
        
	job.setInputFormatClass(TextInputFormat.class);
	job.setOutputFormatClass(TextOutputFormat.class);
        
	FileInputFormat.addInputPath(job, new Path(args[0]));
	FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
	job.waitForCompletion(true);
    }
        
}
