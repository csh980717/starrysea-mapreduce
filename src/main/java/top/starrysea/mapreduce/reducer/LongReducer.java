package top.starrysea.mapreduce.reducer;

import java.io.File;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import top.starrysea.mapreduce.ReduceResult;
import top.starrysea.mapreduce.Reducer;

public abstract class LongReducer extends Reducer {

	private ConcurrentHashMap<String, AtomicLong> reduceResult;

	protected void runReducerTask(File path) {
		ReduceResult<Long> aReduceResult = reduce(path);
		AtomicLong oldResult = new AtomicLong();
		if (reduceResult.containsKey(aReduceResult.getGroup())) {
			oldResult = reduceResult.get(aReduceResult.getGroup());
		}
		oldResult.addAndGet(aReduceResult.getResult());
		reduceResult.put(aReduceResult.getGroup(), oldResult);
	}

	protected abstract ReduceResult<Long> reduce(File path);
}
