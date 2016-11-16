package oneWay;
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

public class Reduce extends MapReduceBase implements Reducer<Text, Text, Text, Text> {

	ArrayList < Text > RelS;
	ArrayList < Text > RelR;
	ArrayList < Text > RelT;

	public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {

		RelS = new ArrayList < Text >() ;
		RelR = new ArrayList < Text >() ;
		RelT = new ArrayList < Text >() ;
		
		while (values.hasNext()) {
			String relationValue = values.next().toString();

			if (relationValue.indexOf('R') >= 0){
				String finalVal = relationValue.substring(1, relationValue.length());
				RelR.add(new Text(finalVal));
			} if (relationValue.indexOf('T') >= 0){
				String finalVal = relationValue.substring(1, relationValue.length());
				RelT.add(new Text(finalVal));

			} else {
				String finalVal = relationValue.substring(1, relationValue.length());
				RelS.add(new Text(finalVal));
			}
		}

		for( Text r : RelR ) {
			for (Text s : RelS) {
				String[] Rtemp = r.toString().split(",");
				String[] Stemp = s.toString().split(",");
				if(Rtemp[1].equalsIgnoreCase(Stemp[0]))
				{
					for (Text t : RelT) {
						String[] Ttemp = t.toString().split(",");
						if(Stemp[1].equalsIgnoreCase(Ttemp[0]))
						{
							output.collect(new Text(), new Text(Rtemp[0]+","+Rtemp[0]+","+Stemp[1]+","+Ttemp[1]));
					}
					}
				}
				
				
				
			}
		}
	}

}