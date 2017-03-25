import java.io.Serializable;

import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

//Aditya this is used find sum of mass double value

public class MassAtomsRDD implements Serializable{

        private JavaRDD<Double> mass;
	public MassAtomsRDD(JavaSparkContext sc, String inputLocation){
		this.setMassRDD(sc.textFile(inputLocation).filter(new GetMassAtoms()).map(new GetMassMapper()));
	}

	public void setMassRDD(JavaRDD<Double> d){
		this.mass = d;
	}
	public JavaRDD<Double> getMassAtomsRDD(){
		return this.mass;
	}
	
}

class GetMassMapper implements Serializable, Function<String, Double>{
	public Double call(String s){
		String[] splitted = s.split("\\s+");
		return Double.parseDouble(splitted[8]);
	}
	
}

class GetMassAtoms implements Function<String, Boolean> {

	  public Boolean call(String s) { 
		  String[] splitted = s.split("\\s+");
		  if(splitted[0].equals("ATOM")){
			  return true;
		  }
		  return false;
	  }
}
