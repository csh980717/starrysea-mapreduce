package top.starrysea.mapreduce;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadPoolExecutor;

import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

public class StarryseaMapReduceManager {

	private ThreadPoolTaskExecutor mapperThreadPool;
	private ThreadPoolTaskExecutor reducerThreadPool;
	private List<MapperAndReduce> mapperAndReduces;

	private String inputPath;
	private String outputPath;

	public StarryseaMapReduceManager(String inputPath, String outputPath) {
		this.inputPath = inputPath;
		this.outputPath = outputPath;
		init();
	}

	private void init() {
		mapperAndReduces = new ArrayList<>();

		// 初始化mapper的线程池
		mapperThreadPool = new ThreadPoolTaskExecutor();
		mapperThreadPool.setCorePoolSize(Runtime.getRuntime().availableProcessors());
		mapperThreadPool.setMaxPoolSize(10);
		mapperThreadPool.setQueueCapacity(25);
		mapperThreadPool.setRejectedExecutionHandler(new ThreadPoolExecutor.CallerRunsPolicy());
		mapperThreadPool.initialize();

		// 初始化reducer的线程池
		reducerThreadPool = new ThreadPoolTaskExecutor();
		reducerThreadPool.setCorePoolSize(Runtime.getRuntime().availableProcessors());
		reducerThreadPool.setMaxPoolSize(10);
		reducerThreadPool.setQueueCapacity(25);
		reducerThreadPool.setRejectedExecutionHandler(new ThreadPoolExecutor.CallerRunsPolicy());
		reducerThreadPool.initialize();
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
