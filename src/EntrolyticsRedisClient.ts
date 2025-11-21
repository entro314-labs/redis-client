import { createClient, RedisClientType } from 'redis';
import debug from 'debug';

export const log = debug('entrolytics:redis-client');

export const DELETED = '__DELETED__';
export const CACHE_PREFIX = 'entrolytics:';

const logError = (err: unknown) => log('Redis error:', err);

export interface EntrolyticsRedisClientOptions {
  url: string;
  prefix?: string;
  defaultTTL?: number;
}

export interface CacheStats {
  hits: number;
  misses: number;
  hitRate: number;
}

export class EntrolyticsRedisClient {
  url: string;
  client: RedisClientType;
  isConnected: boolean;
  prefix: string;
  defaultTTL: number;
  private stats: { hits: number; misses: number };

  constructor({ url, prefix = CACHE_PREFIX, defaultTTL = 3600 }: EntrolyticsRedisClientOptions) {
    const client = createClient({ url }).on('error', logError);

    this.url = url;
    this.client = client as RedisClientType;
    this.isConnected = false;
    this.prefix = prefix;
    this.defaultTTL = defaultTTL;
    this.stats = { hits: 0, misses: 0 };
  }

  private prefixKey(key: string): string {
    return `${this.prefix}${key}`;
  }

  async connect(): Promise<void> {
    if (!this.isConnected) {
      this.isConnected = true;
      await this.client.connect();
      log('Redis connected');
    }
  }

  async disconnect(): Promise<void> {
    if (this.isConnected) {
      await this.client.disconnect();
      this.isConnected = false;
      log('Redis disconnected');
    }
  }

  async get<T = unknown>(key: string): Promise<T | null> {
    await this.connect();

    const data = await this.client.get(this.prefixKey(key));

    try {
      return JSON.parse(data as string) as T;
    } catch {
      return null;
    }
  }

  async set(key: string, value: unknown, ttl?: number): Promise<string | null> {
    await this.connect();

    const serialized = JSON.stringify(value);
    const prefixedKey = this.prefixKey(key);

    if (ttl || this.defaultTTL) {
      return this.client.setEx(prefixedKey, ttl || this.defaultTTL, serialized);
    }

    return this.client.set(prefixedKey, serialized);
  }

  async del(key: string): Promise<number> {
    await this.connect();
    return this.client.del(this.prefixKey(key));
  }

  async incr(key: string): Promise<number> {
    await this.connect();
    return this.client.incr(this.prefixKey(key));
  }

  async decr(key: string): Promise<number> {
    await this.connect();
    return this.client.decr(this.prefixKey(key));
  }

  async expire(key: string, seconds: number): Promise<boolean> {
    await this.connect();
    const result = await this.client.expire(this.prefixKey(key), seconds);
    return result === 1;
  }

  async ttl(key: string): Promise<number> {
    await this.connect();
    return this.client.ttl(this.prefixKey(key));
  }

  async exists(key: string): Promise<boolean> {
    await this.connect();
    const result = await this.client.exists(this.prefixKey(key));
    return result === 1;
  }

  /**
   * Rate limiting helper
   * Returns true if the rate limit has been exceeded
   */
  async rateLimit(key: string, limit: number, windowSeconds: number): Promise<boolean> {
    await this.connect();

    const prefixedKey = this.prefixKey(`ratelimit:${key}`);
    const current = await this.client.incr(prefixedKey);

    if (current === 1) {
      await this.client.expire(prefixedKey, windowSeconds);
    }

    return current > limit;
  }

  /**
   * Get remaining rate limit count
   */
  async getRateLimitRemaining(key: string, limit: number): Promise<number> {
    await this.connect();

    const prefixedKey = this.prefixKey(`ratelimit:${key}`);
    const current = await this.client.get(prefixedKey);
    const count = current ? parseInt(current, 10) : 0;

    return Math.max(0, limit - count);
  }

  /**
   * Cache-through pattern: fetch from cache or execute query and cache result
   */
  async fetch<T>(key: string, query: () => Promise<T>, ttl?: number): Promise<T | null> {
    const result = await this.get<T>(key);

    if (result === DELETED) {
      this.stats.hits++;
      return null;
    }

    if (result !== null) {
      this.stats.hits++;
      return result;
    }

    this.stats.misses++;

    if (query) {
      const data = await query();

      if (data !== null && data !== undefined) {
        await this.store(key, data, ttl);
      }

      return data;
    }

    return null;
  }

  /**
   * Store data in cache with optional TTL
   */
  async store<T>(key: string, data: T, ttl?: number): Promise<string | null> {
    return this.set(key, data, ttl);
  }

  /**
   * Remove from cache - soft delete marks as deleted, hard delete removes entirely
   */
  async remove(key: string, soft = false): Promise<string | number | null> {
    return soft ? this.set(key, DELETED) : this.del(key);
  }

  /**
   * Invalidate multiple keys by pattern
   */
  async invalidatePattern(pattern: string): Promise<number> {
    await this.connect();

    const keys = await this.client.keys(this.prefixKey(pattern));

    if (keys.length === 0) {
      return 0;
    }

    return this.client.del(keys);
  }

  /**
   * Get cache statistics
   */
  getStats(): CacheStats {
    const total = this.stats.hits + this.stats.misses;
    return {
      hits: this.stats.hits,
      misses: this.stats.misses,
      hitRate: total > 0 ? this.stats.hits / total : 0,
    };
  }

  /**
   * Reset cache statistics
   */
  resetStats(): void {
    this.stats = { hits: 0, misses: 0 };
  }

  /**
   * Health check for Redis connection
   */
  async healthCheck(): Promise<{ ok: boolean; latency: number; error?: string }> {
    const start = Date.now();
    try {
      await this.connect();
      await this.client.ping();
      return { ok: true, latency: Date.now() - start };
    } catch (error) {
      return {
        ok: false,
        latency: Date.now() - start,
        error: error instanceof Error ? error.message : 'Unknown error',
      };
    }
  }

  /**
   * Flush all keys with the configured prefix
   */
  async flushPrefix(): Promise<number> {
    return this.invalidatePattern('*');
  }
}

export default EntrolyticsRedisClient;
