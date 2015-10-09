package com.xjd.jedis.lock;

import java.util.Random;

import org.junit.Test;

import redis.clients.jedis.JedisPool;

import com.xjd.jedis.lock.JedisLocker.LockObject;

public class JedisLockerTest {

	@Test
	public void test() {
		JedisPool jedisPool = new JedisPool("103.224.81.184");

		LockThread t1 = new LockThread(jedisPool, "1");
		LockThread t2 = new LockThread(jedisPool, "2");
		LockThread t3 = new LockThread(jedisPool, "3");

		t1.start();
		t2.start();
		t3.start();

		try {
			Thread.sleep(60000L);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		jedisPool.destroy();
	}

	public static class LockThread extends Thread {
		JedisPool jedisPool;
		String name;

		public LockThread(JedisPool jedisPool, String name) {
			this.jedisPool = jedisPool;
			this.name = name;
		}

		@Override
		public void run() {
			while (true) {
				LockObject obj = JedisLocker.lock(jedisPool, "mylock", 10000L, 100, null);
				System.out.println(name + ": LOCK");
				try {
					Thread.sleep(new Random().nextInt(10) * 1000L);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
				JedisLocker.unlock(jedisPool, obj);
				System.out.println(name + ": UNLOCK");
				try {
					Thread.sleep(1000L);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		}

	}
}
