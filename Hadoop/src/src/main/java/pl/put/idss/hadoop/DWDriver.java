package pl.put.idss.hadoop;

import org.apache.hadoop.util.ProgramDriver;

import pl.put.idss.hadoop.examples.WordCount;
import pl.put.idss.hadoop.examples.WordCount2;
import pl.put.idss.hadoop.simpletasks.NGramCount;
import pl.put.idss.hadoop.simpletasks.NeighbourCount;


public class DWDriver {

	public static void main(String[] args) {
		//Shows the list of available programs
		ProgramDriver driver = new ProgramDriver();
		try {
			//You have to add a class of your map-reduce program here
			driver.addClass("wordcount", WordCount.class, "Word Count Example");
			driver.addClass("wordcount2", WordCount2.class, "Word Count 2 Example");
			driver.addClass("ngram", NGramCount.class, "NGram Count Example");
			driver.addClass("neighbourcount", NeighbourCount.class, "Neighbour Count Example");
			
			driver.driver(args);
			System.exit(0);
		} catch (Throwable e) {
			e.printStackTrace();
			System.exit(-1);
		}
	}

}
