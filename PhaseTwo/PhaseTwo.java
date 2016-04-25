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
        
public class PhaseTwo 
{
    public static class Map extends Mapper<LongWritable, Text, Text, Text>
    {
	
	private Text outKey = new Text();

        
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException 
	{
	    String inputLine = value.toString();
	    String temp[] = inputLine.split("\t"); //spliting input string to get pair of word,document name and frequency

	    String docPart[]=temp[0].split(",");//seperating document name and word
	    String docName = docPart[1]; //getting the document number or the document name

	    outKey.set(docName);

	    context.write(outKey,new Text(docPart[0]+","+temp[1]));
	    
	   
	}
    } 
        
    public static class Reduce extends Reducer<Text, Text, Text, Text>
    {

	public void reduce(Text key, Iterable<Text> values, Context context) 
	    throws IOException, InterruptedException
	    {
	    int TotalWords_Doc = 0;
	    ArrayList<Integer> wordFrequencyArray = new ArrayList<Integer>();
			HashMap<String, Integer> hashvariable = new HashMap<String, Integer>();

			for (Text val : values) {
				String[] textvalue = val.toString().split(",");
				hashvariable.put(textvalue[0], Integer.valueOf(textvalue[1]));
				wordFrequencyArray.add(Integer.valueOf(textvalue[1]));
				TotalWords_Doc += Integer.valueOf(textvalue[1]);
			}
			
			int Maxfrequency = Collections.max(wordFrequencyArray);
			
			for (String HashKey : hashvariable.keySet()) {
						
			StringBuilder sbObjOne = new StringBuilder();
			sbObjOne.append(hashvariable.get(HashKey));
			String wfrequency = sbObjOne.toString();
			
			
			StringBuilder sbObjTwo = new StringBuilder();
			sbObjTwo.append(Maxfrequency);
			String MaxFreq = sbObjTwo.toString();
			
			float normalizedFloatValue = Float.parseFloat(wfrequency)/Float.parseFloat(MaxFreq);
			String normalizedValue = Float.toString(normalizedFloatValue);
				context.write(new Text(HashKey + "," + key.toString()),
						new Text(normalizedValue + "," + TotalWords_Doc));
			}
	    }
    }
        
    public static void main(String[] args) throws Exception
    {
	Configuration conf = new Configuration();
        
        @SuppressWarnings("deprecation")
        Job job = new Job(conf, "PhaseTwo");
    
	job.setOutputKeyClass(Text.class);
	job.setOutputValueClass(Text.class);
	job.setJarByClass(PhaseTwo.class);

	job.setMapperClass(Map.class);
	job.setReducerClass(Reduce.class);
        
	job.setInputFormatClass(TextInputFormat.class);
	job.setOutputFormatClass(TextOutputFormat.class);
        
	FileInputFormat.addInputPath(job, new Path(args[0]));
	FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
	job.waitForCompletion(true);
    }
        
}
