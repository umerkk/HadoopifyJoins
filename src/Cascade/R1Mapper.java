package Cascade;
import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

/*
 * **********************************************
 *                                              *
 * Comp 6521 - Advance Database Applications    *
 * Project # 2                                  *
 * Implement Natural Join using Hadoop          *
 *                                              *
 * Developed By:                                *
 * Muhammad Umer (40015021)                     *
 * Hamzah Hamdi                                 *
 *                                              *
 * **********************************************
 */

public class R1Mapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
	 
	public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
         String line = value.toString().trim();
         String[] splitInput = line.split(",");

         Text a = new Text(splitInput[0]);
         Text b = new Text(splitInput[1]);
         Text c = new Text(splitInput[2]);

         Text opString = new Text("R1"+a.toString()+","+b.toString());

         output.collect(c, opString);
     }
	
}
