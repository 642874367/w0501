package com.askyer.kafka.w0501;

import java.io.IOException;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

import org.apache.commons.lang3.RandomUtils;

public class LeaderFairSelector {

	private String lockId;

	private static final String LOCK_ROOT = "/w05lock";
	private final CountDownLatch latch = new CountDownLatch(1);
	private ZooKeeper zkClient;
	private int sessionTimeout;

	public LeaderFairSelector(ZooKeeper zkClient, int sessionTimeout) {
		this.zkClient = zkClient;
		this.sessionTimeout = sessionTimeout;
	}

	public LeaderFairSelector() throws IOException, KeeperException, InterruptedException {
		this.zkClient = ZkClient.getInstance();
		this.sessionTimeout = ZkClient.getSessionTimeout();
	}

	class LockWatcher implements Watcher {
		@Override
		public void process(WatchedEvent event) {
			//if previous node is delete, fire watch event
			if (event.getType() == Event.EventType.NodeDeleted) {
				latch.countDown();
				tryLock();
			}
		}
	}
	


	public synchronized boolean tryLock() {		

		try {	
			// 获得锁根节点下的各锁子节点，并排序
			List<String> nodes = zkClient.getChildren(LOCK_ROOT, true);
			SortedSet<String> sortedNode = new TreeSet<String>();

			for (String node : nodes) {
				sortedNode.add(LOCK_ROOT + "/" + node);
			}

			String first = sortedNode.first();
			SortedSet<String> lessThanMe = sortedNode.headSet(lockId);
			// 检查是否有比当前锁节点lockId更小的节点，若有则监控当前节点的前一节点
			if (lockId.equals(first)) {
				System.out.println(
						"thread " + Thread.currentThread().getName() + " has get the lock, lockId is " + lockId);
				return true;
			} else if (!lessThanMe.isEmpty()) {
				String prevLockId = lessThanMe.last();
				zkClient.exists(prevLockId, new LockWatcher());
				// 当等待sessionTimeout的时间过后，上一个lock的Zookeeper连接会过期，删除所有临时节点，触发监听器
				latch.await(sessionTimeout, TimeUnit.MILLISECONDS);
				
			}

		} catch (Exception e) {
			e.printStackTrace();
		}

		return true;
	}

	public synchronized boolean unlock() {
		//remove current lock
		try {
			System.out.println("thread " + Thread.currentThread().getName() + " unlock the lock: " + lockId
					+ ", the node: " + lockId + " had been deleted");
			zkClient.delete(lockId, -1);
			return true;
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				zkClient.close();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		return false;
	}

	public void registerMyNode() {
		try {
			//createLockRootIfNotExists
			Stat stat = zkClient.exists(LOCK_ROOT, false);//no watcher
			if (stat == null) {
				zkClient.create(LOCK_ROOT, null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
			}
			
			//register this node
			lockId = zkClient.create(LOCK_ROOT + "/", null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
			System.out.println("thread " + Thread.currentThread().getName() + " create the lock node: " + lockId
					+ ", trying to get lock now");
			
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static void main(String[] args) throws IOException, KeeperException, InterruptedException {
		final CountDownLatch latch = new CountDownLatch(10);
		for (int i = 0; i < 10; i++) {
			new Thread(new Runnable() {
				public void run() {
					LeaderFairSelector lock = null;
					try {
						lock = new LeaderFairSelector();
						latch.countDown();
						latch.await();
						lock.registerMyNode();
						lock.tryLock();
						Thread.sleep(RandomUtils.nextInt(2000, 5000));
					} catch (Exception e) {
						e.printStackTrace();
					} finally {
						if (lock != null) {
							lock.unlock();
						}
					}
				}
			}).start();
		}
	}

}