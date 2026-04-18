package ecg;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.Iterator;
import java.util.Scanner;

public class IteratorECG implements Iterator<Integer> {

	public final Scanner scanner;

	public IteratorECG(String file) throws FileNotFoundException {
		scanner = new Scanner(new File(file));
	}

	@Override
	public boolean hasNext() {
		if (scanner.hasNextLine()) {
			return true;
		} else {
			scanner.close();
			return false;
		}
	}

	@Override
	public Integer next() {
		String line = scanner.nextLine();
		String[] fields = line.split(",");
		return Integer.parseInt(fields[1]);
	}
	
}
