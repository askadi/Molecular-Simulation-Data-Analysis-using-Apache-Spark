/* MSApp.java */
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

import scala.Tuple2;

/* MSApp class is the main class to get the files input
 * and generate output files
 */
public class MSApp {

  public static void main(String[] args) {

    // create SparkContext object to access clusters
    SparkConf conf = new SparkConf().setAppName("MS Application").setMaster("local");
    JavaSparkContext sc = new JavaSparkContext(conf);

    // specify input and output location of files
    String inputLoc = "/home/adi/Downloads/data.txt";
    //String outputLoc = "hdfs://localhost:54310/example/output";
    String outputMOI = "/home/adi/Downloads/outputMOI";
    String outputSOM = "/home/adi/Downloads/outputSOM";
    String outputCOM = "/home/adi/Downloads/outputCOM";
    String outputROG = "/home/adi/Downloads/outputROG";
    String outputDM = "/home/adi/Downloads/outputDM";

    // create FrameAtomsRDD out of input data
    FrameAtomsRDD distData = new FrameAtomsRDD(sc, inputLoc);

    // moment of inertia
    VectorRDD MOI = OneBodyQuery.getMOI(distData);
    MOI.getVectorRDD().saveAsTextFile(outputMOI);

    //  sum of mass
    ScalarRDD SOM = OneBodyQuery.getSOM(distData);
    SOM.getScalarRDD().saveAsTextFile(outputSOM);

    MassAtomsRDD distDataM = new MassAtomsRDD(sc, inputLoc);

    //  sum of mass double value
    Double sumOfMass = OneBodyQuery.getMassDouble(distDataM);
    Broadcast<Double> som = sc.broadcast(sumOfMass);

    OneBodyQuery one = new OneBodyQuery(som);

    //  center of mass
    VectorRDD COM = one.getCOM(distData);
    COM.getVectorRDD().saveAsTextFile(outputCOM);

    //  radius of gyration
    ScalarRDD ROG = one.getROG(distData);
    ROG.getScalarRDD().saveAsTextFile(outputROG);

    //  dipole moment
    VectorRDD DM = OneBodyQuery.getDM(distData);
    DM.getVectorRDD().saveAsTextFile(outputDM);

  }

}

class Print implements VoidFunction<Tuple2<Integer, VecCalc>>{

	public void call(Tuple2<Integer, VecCalc> t) throws Exception {
		System.out.print(t._1+" : ");
		t._2().printValues();
		System.out.println();
	}

}

