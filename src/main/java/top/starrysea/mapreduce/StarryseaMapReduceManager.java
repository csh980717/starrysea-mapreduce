package top.starrysea.mapreduce;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class StarryseaMapReduceManager {

	private ExecutorService mapperThreadPool;
	private ExecutorService reducerThreadPool;
	private List<MapperAndReduce> mapperAndReduces;

	private String inputPath;
	private String outputPath;

	public StarryseaMapReduceManager(StarryseaMapReduceConfiguration starryseaMapReduceConfiguration) {
		this.inputPath = starryseaMapReduceConfiguration.getInputPath();
		this.outputPath = starryseaMapReduceConfiguration.getOutputPath();
		init(starryseaMapReduceConfiguration);
	}

	private void init(StarryseaMapReduceConfiguration starryseaMapReduceConfiguration) {
		mapperAndReduces = new ArrayList<>();

		// 初始化mapper的线程池
		mapperThreadPool = new ThreadPoolExecutor(starryseaMapReduceConfiguration.getMapperCorePoolSize(),
				starryseaMapReduceConfiguration.getMapperMaximumPoolSize(), 0L, TimeUnit.MILLISECONDS,
				new LinkedBlockingQueue<Runnable>(),
				starryseaMapReduceConfiguration.getMapperRejectedExecutionHandler());

		// 初始化reducer的线程池
		reducerThreadPool = new ThreadPoolExecutor(starryseaMapReduceConfiguration.getReducerCorePoolSize(),
				starryseaMapReduceConfiguration.getReducerMaximumPoolSize(), 0L, TimeUnit.MILLISECONDS,
				new LinkedBlockingQueue<Runnable>(),
				starryseaMapReduceConfiguration.getReducerRejectedExecutionHandler());
	}

	public StarryseaMapReduceManager register(Mapper mapper, Reducer... reducers) {
		mapper.setInputPath(inputPath);
		mapper.setOutputPath(outputPath);
		mapper.setManagerThreadPool(this::executeMapperTask);
		for (Reducer reducer : reducers) {
			reducer.setManagerThreadPool(this::executeReducerTask);
		}
		mapperAndReduces.add(MapperAndReduce.of(mapper, reducers));
		return this;
	}

	public void run() {
		mapperAndReduces.stream().forEach(mapperAndReduce -> mapperThreadPool.execute(mapperAndReduce.getMapper()));
	}

	private Void executeMapperTask(Runnable task) {
		mapperThreadPool.execute(task);
		return null;
	}

	private Void executeReducerTask(Runnable task) {
		reducerThreadPool.execute(task);
		return null;
	}

}
