package com.xjd.jedis.lock;

import redis.clients.jedis.JedisPool;

import com.xjd.jedis.lock.JedisLocker.LockObject;

public class JedisLockExample {

	public static void main(String[] args) {
		JedisPool jedisPool = new JedisPool("10.0.0.83");

		LockObject obj = null;
		try {
			// 获取锁 "lockKey", 锁失效时间为10000ms, 每次尝试获取锁时间间隔100ms, 无限尝试直到获得锁
			obj = JedisLocker.lock(jedisPool, "lockKey", 10000L, 100L, null);
			// 获取锁 "lockKey", 锁失效时间为10000ms, 每次尝试获取锁时间间隔100ms, 尝试10000ms放弃
			// obj = JedisLocker.lock(jedisPool, "lockKey", 10000L, 100L, 10000L);
			// 获取锁 "lockKey", 锁失效时间为10000ms, 要求立即返回
			// obj = JedisLocker.lock(jedisPool, "lockKey", 10000L, 0L, 0L);

			if (obj == null) {
				System.out.println("获取锁失败!");
				// TODO 做业务处理

			} else {
				System.out.println("获取锁成功: " + obj);
				// TODO 做业务处理
			}
		} finally {
			if (obj != null) {
				// 释放锁
				int unlockResult = JedisLocker.unlock(obj);
				// 或者使用自身的unlock方法
				// obj.unlock();
				// TODO 解锁结果处理, 不处理则忽略
				System.out.println("释放锁完成: " + obj);
			}
		}

		// try-with-resource 用法
		try (LockObject obj2 = JedisLocker.lock(jedisPool, "lockKey", 10000L, 0L, 0L)) {
			if (obj2 == null) {
				System.out.println("获取锁失败!");
				// TODO 做业务处理

			} else {
				System.out.println("获取锁成功: " + obj2);
				// TODO 做业务处理
			}
		}
	}
}
