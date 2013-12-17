import java.util.Comparator;

public class CustomComparator implements Comparator<IdText> {
	@Override
	public int compare(IdText o1, IdText o2) {
		return o1.ID.compareTo(o2.ID);
	}
}