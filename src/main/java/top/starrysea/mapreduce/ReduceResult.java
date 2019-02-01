package top.starrysea.mapreduce;

public class ReduceResult<T> {

	private String group;
	private T result;

	private ReduceResult(String group, T result) {
		this.group = group;
		this.result = result;
	}

	public static <T> ReduceResult<T> of(String group, T result) {
		return new ReduceResult<>(group, result);
	}

	public String getGroup() {
		return group;
	}

	public T getResult() {
		return result;
	}

}
