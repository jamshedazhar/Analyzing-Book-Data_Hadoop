import java.io.IOException;

import org.apache.pig.FilterFunc;
import org.apache.pig.data.Tuple;

public class IsNumber extends FilterFunc {

	public Boolean exec(Tuple input) throws IOException {
		try{
		Integer.parseInt(input.get(0).toString());
		}  catch(Exception e){
			return false;
		}
		return true;
	}
}