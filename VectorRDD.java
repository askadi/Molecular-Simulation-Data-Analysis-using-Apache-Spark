import java.io.Serializable;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function;

/* VectorRDD class to represent vector data type
 * and operations on this data type
 */
public class VectorRDD implements Serializable{

    // frame number is mapped to vector quantity
	private JavaPairRDD<Integer, VecCalc> vec;

    // default constructor
	public VectorRDD(){
		this.vec = null;
	}

    // constructor
	public VectorRDD(JavaPairRDD<Integer, VecCalc> v){
		this.vec = v;
	}

    // setter method
	public void setVectorRDD(JavaPairRDD<Integer, VecCalc> v){
		this.vec = v;
	}

    // getter method
	public JavaPairRDD<Integer, VecCalc> getVectorRDD(){
		return this.vec;
	}

    // convert to string representation
	public JavaPairRDD<Integer, String> getStringVector(){
		return vec.mapValues(new ConvertToStringVector());
	}

}

/* ConvertToStringVector class is used to convert the
 * vectorRDD to string representation
 */
class ConvertToStringVector implements Function<VecCalc, String>{

	public String call(VecCalc vc) throws Exception {
		return vc.toString();
	}

}
