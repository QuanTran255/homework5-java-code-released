package dsl;

import java.text.DecimalFormat;
import java.util.ArrayList;

public class SCollector<A> implements Sink<A> {

	private static DecimalFormat formatter = new DecimalFormat("#,###");

	public ArrayList<A> list = new ArrayList<>();

	@Override
	public void next(A item) {
		list.add(item);
	}

	@Override
	public void end() {
		System.out.println("# output items = " + formatter.format(list.size()));
	}

}
