import debug from 'debug';
import { createClient, type RedisClientType } from 'redis';

export const log = debug('entrolytics:redis-client');

export const DELETED = '__DELETED__';
export const CACHE_PREFIX = 'entrolytics:';

const logError = (err: unknown) => log('Redis error:', err);

export interface EntrolyticsRedisClientOptions {
  url: string;
  prefix?: string;
  defaultTTL?: number;
  /** Max reconnection attempts before giving up */
  maxReconnectAttempts?: number;
  /** Connection timeout in milliseconds */
  connectTimeout?: number;
}

export interface CacheStats {
  hits: number;
  misses: number;
  hitRate: number;
}

/** TTL options - accepts number (seconds) or redis-style options object */
export type TTLOption = number | { EX?: number; PX?: number; EXAT?: number; PXAT?: number };

export class EntrolyticsRedisClient {
  url: string;
  private _client: RedisClientType;
  private _isConnected: boolean;
  private _isConnecting: boolean;
  private _connectPromise: Promise<void> | null;
  prefix: string;
  defaultTTL: number;
  private stats: { hits: number; misses: number };

  constructor({
    url,
    prefix = CACHE_PREFIX,
    defaultTTL = 3600,
    maxReconnectAttempts = 10,
    connectTimeout = 10000,
  }: EntrolyticsRedisClientOptions) {
    const client = createClient({
      url,
      socket: {
        connectTimeout,
        reconnectStrategy: (retries, cause) => {
          if (retries > maxReconnectAttempts) {
            log(`Max reconnect attempts (${maxReconnectAttempts}) exceeded:`, cause?.message);
            return new Error(`Max reconnect attempts exceeded: ${cause?.message}`);
          }
          // Exponential backoff with jitter
          const jitter = Math.floor(Math.random() * 200);
          const delay = Math.min(2 ** retries * 50, 3000);
          log(`Reconnecting in ${delay + jitter}ms (attempt ${retries})`);
          return delay + jitter;
        },
      },
    });

    client.on('error', logError);
    client.on('connect', () => log('Redis connecting...'));
    client.on('ready', () => {
      log('Redis ready');
      this._isConnected = true;
    });
    client.on('end', () => {
      log('Redis connection ended');
      this._isConnected = false;
    });

    this.url = url;
    this._client = client as RedisClientType;
    this._isConnected = false;
    this._isConnecting = false;
    this._connectPromise = null;
    this.prefix = prefix;
    this.defaultTTL = defaultTTL;
    this.stats = { hits: 0, misses: 0 };
  }

  /** Access the underlying redis client for advanced operations */
  get client(): RedisClientType {
    return this._client;
  }

  /** Check if currently connected */
  get isConnected(): boolean {
    return this._isConnected;
  }

  private prefixKey(key: string): string {
    return `${this.prefix}${key}`;
  }

  /** Extract TTL in seconds from TTLOption */
  private extractTTL(ttl?: TTLOption): number | undefined {
    if (ttl === undefined || ttl === null) return undefined;
    if (typeof ttl === 'number') return ttl;
    // Redis-style options object
    if (ttl.EX !== undefined) return ttl.EX;
    if (ttl.PX !== undefined) return Math.ceil(ttl.PX / 1000);
    if (ttl.EXAT !== undefined) return Math.max(0, ttl.EXAT - Math.floor(Date.now() / 1000));
    if (ttl.PXAT !== undefined) return Math.max(0, Math.ceil((ttl.PXAT - Date.now()) / 1000));
    return undefined;
  }

  async connect(): Promise<void> {
    // Already connected
    if (this._isConnected) return;

    // Connection in progress - wait for it
    if (this._isConnecting && this._connectPromise) {
      await this._connectPromise;
      return;
    }

    // Start new connection
    this._isConnecting = true;
    this._connectPromise = this._client.connect().then(
      () => {
        this._isConnected = true;
        this._isConnecting = false;
        log('Redis connected');
      },
      err => {
        this._isConnecting = false;
        this._connectPromise = null;
        throw err;
      },
    );

    await this._connectPromise;
  }

  async disconnect(): Promise<void> {
    if (this._isConnected) {
      await this._client.disconnect();
      this._isConnected = false;
      this._isConnecting = false;
      this._connectPromise = null;
      log('Redis disconnected');
    }
  }

  async get<T = unknown>(key: string): Promise<T | null> {
    try {
      await this.connect();
      const data = await this._client.get(this.prefixKey(key));

      if (data === null) return null;

      try {
        return JSON.parse(data) as T;
      } catch {
        // If JSON parse fails, return as-is (might be a plain string)
        return data as T;
      }
    } catch (err) {
      log('Redis get error:', err);
      return null;
    }
  }

  /**
   * Set a value in cache
   * @param key - Cache key
   * @param value - Value to cache (will be JSON serialized)
   * @param ttl - TTL as number (seconds) or options object { EX?: number, PX?: number }
   */
  async set(key: string, value: unknown, ttl?: TTLOption): Promise<string | null> {
    try {
      await this.connect();

      const serialized = JSON.stringify(value);
      const prefixedKey = this.prefixKey(key);
      const ttlSeconds = this.extractTTL(ttl) ?? this.defaultTTL;

      if (ttlSeconds > 0) {
        return this._client.setEx(prefixedKey, ttlSeconds, serialized);
      }

      return this._client.set(prefixedKey, serialized);
    } catch (err) {
      log('Redis set error:', err);
      return null;
    }
  }

  async del(key: string): Promise<number> {
    try {
      await this.connect();
      return this._client.del(this.prefixKey(key));
    } catch (err) {
      log('Redis del error:', err);
      return 0;
    }
  }

  async incr(key: string): Promise<number> {
    try {
      await this.connect();
      return this._client.incr(this.prefixKey(key));
    } catch (err) {
      log('Redis incr error:', err);
      return 0;
    }
  }

  async decr(key: string): Promise<number> {
    try {
      await this.connect();
      return this._client.decr(this.prefixKey(key));
    } catch (err) {
      log('Redis decr error:', err);
      return 0;
    }
  }

  async expire(key: string, seconds: number): Promise<boolean> {
    try {
      await this.connect();
      return await this._client.expire(this.prefixKey(key), seconds);
    } catch (err) {
      log('Redis expire error:', err);
      return false;
    }
  }

  async ttl(key: string): Promise<number> {
    try {
      await this.connect();
      return this._client.ttl(this.prefixKey(key));
    } catch (err) {
      log('Redis ttl error:', err);
      return -1;
    }
  }

  async exists(key: string): Promise<boolean> {
    try {
      await this.connect();
      const result = await this._client.exists(this.prefixKey(key));
      return result === 1;
    } catch (err) {
      log('Redis exists error:', err);
      return false;
    }
  }

  /**
   * Rate limiting helper
   * Returns true if the rate limit has been exceeded
   */
  async rateLimit(key: string, limit: number, windowSeconds: number): Promise<boolean> {
    try {
      await this.connect();

      const prefixedKey = this.prefixKey(`ratelimit:${key}`);
      const current = await this._client.incr(prefixedKey);

      if (current === 1) {
        await this._client.expire(prefixedKey, windowSeconds);
      }

      return current > limit;
    } catch (err) {
      log('Redis rateLimit error:', err);
      return false; // Fail open - allow the request if Redis is down
    }
  }

  /**
   * Get remaining rate limit count
   */
  async getRateLimitRemaining(key: string, limit: number): Promise<number> {
    try {
      await this.connect();

      const prefixedKey = this.prefixKey(`ratelimit:${key}`);
      const current = await this._client.get(prefixedKey);
      const count = current ? parseInt(current, 10) : 0;

      return Math.max(0, limit - count);
    } catch (err) {
      log('Redis getRateLimitRemaining error:', err);
      return limit; // Return full limit if Redis is down
    }
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
   * Invalidate multiple keys by pattern using SCAN (production-safe, non-blocking)
   */
  async invalidatePattern(pattern: string): Promise<number> {
    try {
      await this.connect();

      const prefixedPattern = this.prefixKey(pattern);
      let cursor = 0;
      let deletedCount = 0;

      do {
        const result = await this._client.scan(cursor, {
          MATCH: prefixedPattern,
          COUNT: 100,
        });
        cursor = result.cursor;

        if (result.keys.length > 0) {
          deletedCount += await this._client.del(result.keys);
        }
      } while (cursor !== 0);

      return deletedCount;
    } catch (err) {
      log('Redis invalidatePattern error:', err);
      return 0;
    }
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
      await this._client.ping();
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
