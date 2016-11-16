package Cascade;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

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

public class J2Reducer extends MapReduceBase implements Reducer<Text, Text, Text, Text> {

	ArrayList < Text > RelR1;
	ArrayList < Text > RelT; 

	public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {

		RelR1 = new ArrayList < Text >() ;
		RelT = new ArrayList < Text >() ;
		
		while (values.hasNext()) {
			String relationValue = values.next().toString();

			if (relationValue.indexOf("R1") >= 0){
				String finalVal = relationValue.substring(2, relationValue.length());
				RelR1.add(new Text(finalVal));
			} else {
				String finalVal = relationValue.substring(1, relationValue.length());
				RelT.add(new Text(finalVal));
			}
		}

		for( Text r : RelR1 ) {
			for (Text s : RelT) {
				output.collect(new Text(), new Text(r + "," + key.toString() + "," + s));
			}
		}
	}

}