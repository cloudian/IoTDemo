import java.io.BufferedReader;
import java.io.InputStreamReader;

public class GetData {
	private static String line;
	public static String getTemp() throws Exception {
		Runtime rt = Runtime.getRuntime();
                Process p = rt.exec("python /home/pi/DHT11_Python/trial.py");
		//Process p = rt.exec("echo /home/pi/DHT11_Python/trial.py");
		BufferedReader bri = new BufferedReader(new InputStreamReader(p.getInputStream()));
		if ((line = bri.readLine()) != null) {
			System.out.println(line);
		} else {
			System.out.println("This failed");
		}
		bri.close();
		return line;
	}



}

