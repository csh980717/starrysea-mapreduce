package top.starrysea.mapreduce;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.function.Function;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class Reducer implements Runnable {

	protected final Logger logger = LoggerFactory.getLogger(this.getClass());
	protected MapReduceContext context;
	protected Function<Runnable, Void> managerThreadPool;

	@Override
	public final void run() {
		String fileNameWithoutExtension = getFileName().substring(0, getFileName().lastIndexOf('.'));
		analyze(getInputPath() + "/" + fileNameWithoutExtension + "/" + context.getOutputFileSubType());
	}

	public String getInputPath() {
		return context.getOutputPath();
	}

	public String getFileName() {
		return context.getOutputFileName() + "." + context.getOutputFileSubType();
	}

	public void setContext(MapReduceContext context) {
		this.context = context;
	}

	public void setManagerThreadPool(Function<Runnable, Void> managerThreadPool) {
		this.managerThreadPool = managerThreadPool;
	}

	private void analyze(String fileDirectory) {
		List<File> fileList = new ArrayList<>();
		File rootDir = new File(fileDirectory);
		File[] files = rootDir.listFiles();
		for (File i : files) {
			if (i.isFile())
				fileList.add(i);
		}
		CountDownLatch countDownLatch = new CountDownLatch(fileList.size());
		fileList.stream().forEach(f -> managerThreadPool.apply(new ReduceTask(f, countDownLatch)));
		try {
			countDownLatch.await();
			handleReduceResult();
		} catch (InterruptedException e) {
			logger.error(e.getMessage(), e);
			Thread.currentThread().interrupt();
		}
	}

	private class ReduceTask implements Runnable {
		private File path;
		private CountDownLatch countDownLatch;

		ReduceTask(File path, CountDownLatch countDownLatch) {
			this.path = path;
			this.countDownLatch = countDownLatch;
		}

		@Override
		public void run() {
			runReducerTask(path);
			countDownLatch.countDown();
		}
	}

	protected abstract void runReducerTask(File path);

	protected abstract void handleReduceResult();

}
