/* UserInterface.java */
import org.apache.spark.api.java.*;

import java.io.Serializable;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.Function3;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.api.java.JavaRDDLike;
import scala.Tuple2;
import java.lang.Math;
import java.util.List;
import java.util.Scanner;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.io.File;
import java.io.FilenameFilter;

// This program allows the user to easily look into the Moment of Inertia, Sum of Mass, Center of Mass, Radius of Gyration, Dipole Moment values in desired frame

public class UserInterface {

  public static void main(String[] args) throws IOException, ArrayIndexOutOfBoundsException {
    String outputMOI = "/home/adi/Downloads/outputMOI/";
    String outputSOM = "/home/adi/Downloads/outputSOM/";
    String outputCOM = "/home/adi/Downloads/outputCOM/";
    String outputROG = "/home/adi/Downloads/outputROG/";
    String outputDM = "/home/adi/Downloads/outputDM/";
    
    Scanner scanner = new Scanner(System.in);

    //  prompt for the option
    System.out.print("Enter the option: \n 1 - Moment of Inertia\n 2 - Sum of Mass\n 3 - Center of Mass\n 4 - Radius of Gyration\n 5 - Dipole Moment \n");
  
    String option = scanner.next();
    int opt = Integer.parseInt(option);
    // check for invalid option
    if(opt <1 || opt > 5)
      System.out.println("invalid option");
    else{
		
    	//  prompt for the frame number
      	System.out.print("Enter the Frame number: ");
        String frame_no = scanner.next();
        int part_no = ((Integer.parseInt(frame_no)) % 60);
        String frame_string = new String("(" + frame_no);
       File topDir;
       boolean frame_not_found = true;
		
	// Now we know that user has entered valid option and frame number,
	// We Scan through all the file names in the appropriate directory, if there is a match then Read the file line by line.
        // Once the frame number has been found then print the Moment of Inertia, Sum of Mass, Center of Mass, Radius of gyration and Dipole  		//  Moment value to console

        switch(opt){
		case 1: 
                        topDir = new File(outputMOI);
		       for (File file:topDir.listFiles() ){
			    String[] filename_parts = file.getName().split("-");
                            if (filename_parts.length >1 && !file.getName().contains(".crc") && frame_not_found){
                          	  int filename_suffix = Integer.parseInt(filename_parts[1]);
                         	   String line = null;
                          	  if(part_no == filename_suffix){
                          		BufferedReader in = new BufferedReader(new FileReader(file));
                             	 	 line = in.readLine();
                              		  while(line != null && frame_not_found){
                                		if(line.contains(frame_string)){
							String[] line_split = line.split(",");
                                              	  System.out.println(" Moment of Inertia is (" + line_split[1]);
							frame_not_found = false;
                                       		 }
                                            line = in.readLine();
                               		 }
                               		 in.close();
                            	}
				}
                       }
                       break;
                case 2:
                        topDir = new File(outputSOM);
		       for (File file:topDir.listFiles() ){
			    String[] filename_parts = file.getName().split("-");
                            if (filename_parts.length >1 && !file.getName().contains(".crc") && frame_not_found){
                          	  int filename_suffix = Integer.parseInt(filename_parts[1]);
                         	   String line = null;
                          	  if(part_no == filename_suffix){
                          		BufferedReader in = new BufferedReader(new FileReader(file));
                             	 	 line = in.readLine();
                              		  while(line != null && frame_not_found){
                                		if(line.contains(frame_string)){
							String[] line_split = line.split(",");
                                              	  System.out.println(" Sum of Mass is (" + line_split[1]);
							frame_not_found = false;
                                       		 }
                                            line = in.readLine();
                               		 }
                               		 in.close();
                            	}
				}
                       }

                       break;
		case 3:
                        topDir = new File(outputCOM);
		       for (File file:topDir.listFiles() ){
			    String[] filename_parts = file.getName().split("-");
                            if (filename_parts.length >1 && !file.getName().contains(".crc") && frame_not_found){
                          	  int filename_suffix = Integer.parseInt(filename_parts[1]);
                         	   String line = null;
                          	  if(part_no == filename_suffix){
                          		BufferedReader in = new BufferedReader(new FileReader(file));
                             	 	 line = in.readLine();
                              		  while(line != null && frame_not_found){
                                		if(line.contains(frame_string)){
							String[] line_split = line.split(",");
                                              	  System.out.println(" Center of Mass is (" + line_split[1]);
							frame_not_found = false;
                                       		 }
                                            line = in.readLine();
                               		 }
                               		 in.close();
                            	}
				}
                       }

		       break;
                case 4:
                        topDir = new File(outputROG);
		       for (File file:topDir.listFiles() ){
			    String[] filename_parts = file.getName().split("-");
                            if (filename_parts.length >1 && !file.getName().contains(".crc") && frame_not_found){
                          	  int filename_suffix = Integer.parseInt(filename_parts[1]);
                         	   String line = null;
                          	  if(part_no == filename_suffix){
                          		BufferedReader in = new BufferedReader(new FileReader(file));
                             	 	 line = in.readLine();
                              		  while(line != null && frame_not_found){
                                		if(line.contains(frame_string)){
							String[] line_split = line.split(",");
                                              	  System.out.println(" Radius of gyration is (" + line_split[1]);
							frame_not_found = false;
                                       		 }
                                            line = in.readLine();
                               		 }
                               		 in.close();
                            	}
				}
                       }

                       break;
		case 5:
                        topDir = new File(outputDM);
		       for (File file:topDir.listFiles() ){
			    String[] filename_parts = file.getName().split("-");
                            if (filename_parts.length >1 && !file.getName().contains(".crc") && frame_not_found){
                          	  int filename_suffix = Integer.parseInt(filename_parts[1]);
                         	   String line = null;
                          	  if(part_no == filename_suffix){
                          		BufferedReader in = new BufferedReader(new FileReader(file));
                             	 	 line = in.readLine();
                              		  while(line != null && frame_not_found){
                                		if(line.contains(frame_string)){
							String[] line_split = line.split(",");
                                              	  System.out.println(" Dipole moment is (" + line_split[1]);
							frame_not_found = false;
                                       		 }
                                            line = in.readLine();
                               		 }
                               		 in.close();
                            	}
				}
                       }

		       break;
        
	}
	
    }   
 
  }

}

