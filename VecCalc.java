import java.io.Serializable;

/* VecCalc class to support VectorRDD
 * by implementing functions for
 * operations on vectors
 */
class VecCalc implements Serializable{

    // variables representing vector
	public double x, y, z;

    // default constructor
	public VecCalc(){
		x = 0; y = 0; z = 0;
	}

    // constructor
	public VecCalc(double a, double b, double c){
		x = a; y = b; z = c;
	}

    // add two vectors
	public VecCalc add(VecCalc mc){
		return new VecCalc(x+mc.x, y+mc.y, z+mc.z);
	}

    // print the values of vectors
	public void printValues(){
		System.out.print("x: "+x+"y: "+y+"z: "+z);
	}

    // convert vector to string representation
	public String toString(){
		return (Double.toString(x)+" "+Double.toString(y)+" "+Double.toString(z));
	}

}



