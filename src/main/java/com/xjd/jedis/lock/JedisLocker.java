package com.xjd.jedis.lock;

import java.util.Random;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.Transaction;

/**
 * Jedis分布式锁
 * @author elvis.xu
 * @since 2015-10-08 23:53
 */
public class JedisLocker {

	public static Random random = new Random();

	public static String genVal() {
		return System.currentTimeMillis() + "" + random.nextInt(10);
	}

	public static LockObject lock(JedisPool jedisPool, String key, Long expireMilliseconds, long tryLockMilliseconds,
			Long timeoutMilliseconds) {
		return lock(jedisPool, key, genVal(), expireMilliseconds, tryLockMilliseconds, timeoutMilliseconds);
	}

	public static LockObject lock(JedisPool jedisPool, String key, String val, Long expireMilliseconds, long tryLockMilliseconds,
			Long timeoutMilliseconds) {
		JedisResource jedisResource = new JedisResource(jedisPool);
		return lock(jedisResource, key, genVal(), expireMilliseconds, tryLockMilliseconds, timeoutMilliseconds);
	}

	/**
	 * <pre>
	 * 会占用jedis链接资源, 推荐使用JedisPool
	 * </pre>
	 * @param jedis
	 * @param key
	 * @param expireMilliseconds
	 * @param tryLockMilliseconds
	 * @param timeoutMilliseconds
	 * @return
	 */
	public static LockObject lock(Jedis jedis, String key, Long expireMilliseconds, long tryLockMilliseconds,
			Long timeoutMilliseconds) {
		return lock(jedis, key, genVal(), expireMilliseconds, tryLockMilliseconds, timeoutMilliseconds);
	}

	/**
	 * <pre>
	 * 会占用jedis链接资源, 推荐使用JedisPool
	 * </pre>
	 * @param jedis
	 * @param key
	 * @param val
	 * @param expireMilliseconds
	 * @param tryLockMilliseconds
	 * @param timeoutMilliseconds
	 * @return
	 */
	public static LockObject lock(Jedis jedis, String key, String val, Long expireMilliseconds, long tryLockMilliseconds,
			Long timeoutMilliseconds) {
		JedisResource jedisResource = new JedisResource(jedis);
		return lock(jedisResource, key, genVal(), expireMilliseconds, tryLockMilliseconds, timeoutMilliseconds);
	}

	/**
	 * <pre>
	 * 获取分布式锁
	 * </pre>
	 * @param jedisResource
	 * @param key
	 *            锁的key值
	 * @param val
	 *            锁的value值
	 * @param expireMilliseconds
	 *            锁失效时间, null表示永不失效
	 * @param tryLockMilliseconds
	 *            如果获取锁失败,多久后重试
	 * @param timeoutMilliseconds
	 *            获取锁的最大等待时间, null表示一直等待
	 * @return 获取锁成功返回对象, 获取锁失败(超时)返回null
	 */
	protected static LockObject lock(JedisResource jedisResource, String key, String val, Long expireMilliseconds,
			long tryLockMilliseconds, Long timeoutMilliseconds) {
		String result = null;
		boolean ok = false;
		long exptime, stime, starttime = -1, remaintime = 0;
		Jedis jedis = null, tmpJedis = null;
		if (timeoutMilliseconds != null) {
			starttime = System.currentTimeMillis();
		}
		try {
			jedis = jedisResource.getResource();
			while (true) {
				// 1
				if (expireMilliseconds == null) {
					result = jedis.set(key, val, "nx");
				} else {
					result = jedis.set(key, val, "nx", "px", expireMilliseconds);
				}
				if (result != null) {
					ok = true;
					break;
				}
				if (timeoutMilliseconds != null && (timeoutMilliseconds - (System.currentTimeMillis() - starttime) <= 0)) {
					ok = false;
					break;
				}
				// 2
				exptime = jedis.pttl(key);
				if (exptime == -2) {
					continue;
				} else {
					tmpJedis = jedis;
					jedis = null;
					jedisResource.releaseResource(tmpJedis);
					tmpJedis = null;
					stime = tryLockMilliseconds;
					if (exptime >= 0 && exptime < stime) {
						stime = exptime;
					}
					if (timeoutMilliseconds != null) {
						remaintime = timeoutMilliseconds - (System.currentTimeMillis() - starttime);
						if (remaintime <= 0) {
							remaintime = 0;
						}
						if (remaintime < stime) {
							stime = remaintime;
						}
					}
					try {
						Thread.sleep(stime);
					} catch (InterruptedException e) {
						// do-nothing
					}
					jedis = jedisResource.getResource();
				}
			}
		} finally {
			if (jedis != null) {
				jedisResource.releaseResource(jedis);
			}
		}

		LockObject lockObject = null;
		if (ok) {
			lockObject = new LockObject();
			lockObject.key = key;
			lockObject.val = val;
			lockObject.expireMilliseconds = expireMilliseconds;
			lockObject.jedisResource = jedisResource;
			lockObject.unlocked = false;
		}
		return lockObject;
	}

	/**
	 * <pre>
	 * 使用获得锁相同的Jedis资源来释放锁
	 * </pre>
	 * @param lockObject
	 * @return
	 */
	public static int unlock(LockObject lockObject) {
		Jedis jedis = null;
		try {
			jedis = lockObject.getJedisResource().getResource();
			return unlock(jedis, lockObject);
		} finally {
			if (jedis != null) {
				lockObject.getJedisResource().releaseResource(jedis);
			}
		}
	}

	/**
	 * <pre>
	 * 释放锁
	 * </pre>
	 * @param jedisPool
	 * @param lockObject
	 * @return 正常解锁, 返回0; 锁过期, 返回1; 锁已被它人获取, 返回2
	 */
	public static int unlock(JedisPool jedisPool, LockObject lockObject) {
		Jedis jedis = null;
		try {
			jedis = jedisPool.getResource();
			return unlock(jedis, lockObject);
		} finally {
			if (jedis != null) {
				jedis.close();
			}
		}
	}

	public static int unlock(Jedis jedis, LockObject lockObject) {
		if (lockObject.isUnlocked()) {
			return lockObject.unlockRt;
		}
		int unlockRt = -1;
		Long pttl = jedis.pttl(lockObject.getKey());
		if (pttl == -2) {
			unlockRt = 1;

		} else {
			while (true) {
				jedis.watch(lockObject.getKey());
				try {
					String val = jedis.get(lockObject.getKey());
					if (val == null) {
						unlockRt = 1;
						break;
					}
					if (!val.equals(lockObject.getVal())) {
						unlockRt = 2;
						break;
					}
					Transaction trans = jedis.multi();
					trans.del(lockObject.getKey());
					Long rt = (Long) trans.exec().get(0);
					if (rt == null) {
						unlockRt = 2;
						break;
					} else if (rt == 0) {
						unlockRt = 1;
						break;
					} else {
						unlockRt = 0;
						break;
					}
				} finally {
					jedis.unwatch();
				}
			}
		}
		lockObject.unlocked = true;
		lockObject.unlockRt = unlockRt;
		return unlockRt;
	}

	public static class LockObject implements AutoCloseable {
		protected String key;
		protected String val;
		protected Long expireMilliseconds;

		protected JedisResource jedisResource;
		protected boolean unlocked;
		protected int unlockRt = -1;

		public String getKey() {
			return key;
		}

		public String getVal() {
			return val;
		}

		public Long getExpireMilliseconds() {
			return expireMilliseconds;
		}

		public JedisResource getJedisResource() {
			return jedisResource;
		}

		public boolean isUnlocked() {
			return unlocked;
		}

		public int getUnlockRt() {
			return unlockRt;
		}

		public int unlock() {
			return JedisLocker.unlock(this);
		}

		public int unlock(JedisPool jedisPool) {
			return JedisLocker.unlock(jedisPool, this);
		}

		public int unlock(Jedis jedis) {
			return JedisLocker.unlock(jedis, this);
		}

		@Override
		public void close() {
			unlock();
		}

		@Override
		public String toString() {
			return "LockObject [key=" + key + ", val=" + val + ", expireMilliseconds=" + expireMilliseconds + ", unlocked="
					+ unlocked + ", unlockRt=" + unlockRt + "]";
		}

	}

	protected static class JedisResource {
		protected JedisPool jedisPool;
		protected Jedis jedis;

		public JedisResource(JedisPool jedisPool) {
			this.jedisPool = jedisPool;
		}

		public JedisResource(Jedis jedis) {
			this.jedis = jedis;
		}

		public Jedis getResource() {
			if (jedis != null) {
				return jedis;
			}
			return jedisPool.getResource();
		}

		public void releaseResource(Jedis jedis) {
			if (jedis != this.jedis) {
				jedis.close();
			}
		}
	}
}
