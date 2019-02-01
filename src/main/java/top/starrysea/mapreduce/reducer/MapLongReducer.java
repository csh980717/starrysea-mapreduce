package top.starrysea.mapreduce.reducer;

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import top.starrysea.mapreduce.MapReduceContext;
import top.starrysea.mapreduce.ReduceResult;
import top.starrysea.mapreduce.Reducer;

public abstract class MapLongReducer extends Reducer {

	private ConcurrentHashMap<String, Map<String, AtomicLong>> reduceResult = new ConcurrentHashMap<>();

	@Override
	protected void runReducerTask(File path) {
		ReduceResult<Map<String, Long>> aReduceResult = reduce(path);
		Map<String, AtomicLong> oldResult = new HashMap<>();
		if (reduceResult.containsKey(aReduceResult.getGroup())) {
			oldResult = reduceResult.get(aReduceResult.getGroup());
			for (Map.Entry<String, AtomicLong> entry : oldResult.entrySet()) {
				if (aReduceResult.getResult().containsKey(entry.getKey())) {
					entry.getValue().addAndGet(aReduceResult.getResult().get(entry.getKey()));
				}
			}
		}
		reduceResult.put(aReduceResult.getGroup(), oldResult);
	}

	@Override
	protected void handleReduceResult() {
		Map<String, Map<String, Long>> finalResult = new HashMap<>();
		for (Map.Entry<String, Map<String, AtomicLong>> entry : reduceResult.entrySet()) {
			Map<String, AtomicLong> value = entry.getValue();
			Map<String, Long> resultValue = new HashMap<>();
			for (Map.Entry<String, AtomicLong> inEntry : value.entrySet()) {
				resultValue.put(inEntry.getKey(), inEntry.getValue().get());
			}
			finalResult.put(entry.getKey(), resultValue);
		}
		reduceFinish(finalResult, context);
		reduceResult.clear();
	}

	protected abstract ReduceResult<Map<String, Long>> reduce(File path);

	protected abstract void reduceFinish(Map<String, Map<String, Long>> reduceResult, MapReduceContext context);
}
