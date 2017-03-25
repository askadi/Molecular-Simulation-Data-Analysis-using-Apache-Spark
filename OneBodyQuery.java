import java.io.Serializable;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.broadcast.Broadcast;

public class OneBodyQuery {

    // sum of masses needs to be broadcasted, as it is necessary for computing center of mass and radius of gyration
    Broadcast <Double> som;

    // constructor
    public OneBodyQuery(Broadcast <Double> sumOfMass){
            som = sumOfMass;
    }

    // moment of inertia
	public static VectorRDD getMOI(FrameAtomsRDD f){
		return new VectorRDD(f.getFrameAtomsRDD().mapValues(new ConvertToMOIVec()).reduceByKey(new AddVec()));
	}

    // dipole moment
	public static VectorRDD getDM(FrameAtomsRDD f){
		return new VectorRDD(f.getFrameAtomsRDD().mapValues(new ConvertToDMVec()).reduceByKey(new AddVec()));
	}

    // center of mass
	public VectorRDD getCOM(FrameAtomsRDD f){
		return new VectorRDD(f.getFrameAtomsRDD().mapValues(new ConvertToCOMVec(som)).reduceByKey(new AddVec()));
	}

    // mass
	public static Double getMassDouble(MassAtomsRDD m){
		return (m.getMassAtomsRDD().reduce(new AddScalar()));
	}

    // sum of mass
	public static ScalarRDD getSOM(FrameAtomsRDD f){
		return new ScalarRDD(f.getFrameAtomsRDD().mapValues(new ConvertToMass()).reduceByKey(new AddScalar()));
	}

    // radius of gyration
    public ScalarRDD getROG(FrameAtomsRDD f){
		return new ScalarRDD(f.getFrameAtomsRDD().mapValues(new ConvertToROG(som)).reduceByKey(new AddScalar()));
	}

}


class ConvertToDMVec implements Function<Double[], VecCalc>{

    //  Dipole moment is determined from charge of atom

	public VecCalc call(Double[] d){
		VecCalc mc = new VecCalc();
		mc.x = d[2]*d[5];
		mc.y = d[3]*d[5];
		mc.z = d[4]*d[5];
		return mc;
	}

}

class ConvertToMOIVec implements Function<Double[], VecCalc>{

    // Moment of Inertia is determined

	public VecCalc call(Double[] d) throws Exception {
		VecCalc v = new VecCalc();
		v.x = d[2]*d[6];
		v.y = d[3]*d[6];
		v.z = d[4]*d[6];
		return v;
	}

}

class ConvertToCOMVec implements Serializable, Function<Double[], VecCalc>{

        //  Center of Mass = MOI / Sum of Mass

        Broadcast <Double> sumOfMass;
        public ConvertToCOMVec(Broadcast <Double> som){
             sumOfMass = som;
        }
        public VecCalc call(Double[] d) throws Exception {
	        VecCalc v = new VecCalc();
            v.x = d[2]*d[6]/sumOfMass.value();
            v.y = d[3]*d[6]/sumOfMass.value();
            v.z = d[4]*d[6]/sumOfMass.value();
            return v;
        }

}

class ConvertToROG implements Serializable, Function<Double[], Double>{

        //  Radius of gyration = Center of mass / sum of mass

        Broadcast <Double> sumOfMass;
        public ConvertToROG(Broadcast <Double> som){
             sumOfMass = som;
        }
        public  Double call(Double[] d) throws Exception {
            return (d[4]*d[6])/(sumOfMass.value()*sumOfMass.value());
        }

}

class ConvertToMass implements Function<Double[], Double>{

    //  sum of mass

	public  Double call(Double[] d) throws Exception {
		return d[6];
	}

}

class AddScalar implements Function2<Double, Double, Double>{

    // this is used to determine sum of mass and radius of gyration

	public Double call(Double d1, Double d2) throws Exception{
		return d1 + d2;
	}

}

class AddVec implements Function2<VecCalc, VecCalc, VecCalc>{

    // function facilitating addition of two vectors

	public VecCalc call(VecCalc mc1, VecCalc mc2) throws Exception {
		return mc1.add(mc2);
	}

}

