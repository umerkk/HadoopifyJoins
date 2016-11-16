package oneWay;
import java.util.Date;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapred.lib.MultipleInputs;


/*
 * **********************************************
 *                                              *
 * Comp 6521 - Advance Database Applications    *
 * Project # 1                                  *
 * Implement Natural Join using Hadoop          *
 *                                              *
 * Developed By:                                *
 * Muhammad Umer (40015021)                     *
 * Hamzah Hamdi                                 *
 *                                              *
 * **********************************************
 */

public class Main {

    public static void main(String[] args) throws Exception {
    	long start = new Date().getTime();
    	 
        JobConf conf = new JobConf(MultipleInputs.class);
        conf.setJobName("Comp 6521 Project 2 - Natural Join");

        conf.setOutputFormat(SequenceFileOutputFormat.class);
        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(Text.class);
        
        conf.setReducerClass(Reduce.class);

        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);
        

        conf.setNumReduceTasks(1);
	     
        MultipleInputs.addInputPath(conf, new Path(args[0]), TextInputFormat.class, RMapper.class);
        MultipleInputs.addInputPath(conf, new Path(args[1]), TextInputFormat.class, SMapper.class);
        MultipleInputs.addInputPath(conf, new Path(args[2]), TextInputFormat.class, TMapper.class);


        FileOutputFormat.setOutputPath(conf, new Path("OneWayOutput/"));
        JobClient.runJob(conf);

        
        long end = new Date().getTime();
        System.out.println("Job took "+(end-start) + "milliseconds");

    }

}
