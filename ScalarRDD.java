import java.io.Serializable;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function;

/* ScalarRDD class to represent scalar data type
 * and operations on this data type
 */
public class ScalarRDD implements Serializable{

    // frame number is mapped to scalar quantity
	private JavaPairRDD<Integer, Double> scalarQuantity;

    // default constructor
	public ScalarRDD(){
		this.scalarQuantity = null;
	}

    // constructor
	public ScalarRDD(JavaPairRDD<Integer, Double> d){
		this.scalarQuantity = d;
	}

    // setter method
	public void setScalarRDD(JavaPairRDD<Integer, Double> d){
		this.scalarQuantity = d;
	}

    // getter method
	public JavaPairRDD<Integer, Double> getScalarRDD(){
		return this.scalarQuantity;
	}

    // convert to string representation
	public JavaPairRDD<Integer, String> getStringScalar(){
		return this.scalarQuantity.mapValues(new ConvertToStringScalar());
	}

}

/* ConvertToDoubleScalar class is used to convert the
 * scalarRDD to double representation
 */
class ConvertToDoubleScalar implements Function<Double, Double>{

	public Double call(Double d1) throws Exception {
		return d1;
	}

}

/* ConvertToStringScalar class is used to convert the
 * scalarRDD to string representation
 */
class ConvertToStringScalar implements Function<Double, String>{

	public String call(Double d1) throws Exception {
		return Double.toString(d1);
	}

}

