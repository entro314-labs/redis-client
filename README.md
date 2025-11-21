# @entro314labs/redis-client

Redis client wrapper with caching utilities, rate limiting, and cache-through patterns for Entrolytics.

## Installation

```bash
pnpm add @entro314labs/redis-client
```

## Features

- **Cache-through Pattern**: Automatically cache query results
- **Rate Limiting**: Built-in rate limiting with configurable windows
- **Key Prefixing**: Automatic key prefixing to avoid collisions
- **Cache Statistics**: Track hit/miss rates
- **Health Checks**: Built-in Redis health monitoring
- **Soft Deletes**: Mark items as deleted without removing from cache
- **Pattern Invalidation**: Bulk invalidate keys by pattern
- **Debug Logging**: Optional logging via `debug` package

## Usage

### Basic Setup

```typescript
import { EntrolyticsRedisClient } from '@entro314labs/redis-client';

const redis = new EntrolyticsRedisClient({
  url: process.env.REDIS_URL,
  prefix: 'myapp:',
  defaultTTL: 3600, // 1 hour
});
```

### Cache-through Pattern

```typescript
// Fetch from cache or execute query
const user = await redis.fetch(
  `user:${userId}`,
  () => db.query.users.findFirst({ where: eq(users.id, userId) }),
  86400 // Cache for 24 hours
);
```

### Rate Limiting

```typescript
// Returns true if rate limit exceeded
const isLimited = await redis.rateLimit(
  `api:${ip}`,
  100,  // 100 requests
  60    // per 60 seconds
);

if (isLimited) {
  return new Response('Too Many Requests', { status: 429 });
}

// Get remaining requests
const remaining = await redis.getRateLimitRemaining(`api:${ip}`, 100);
```

### Basic Operations

```typescript
// Set with TTL
await redis.set('key', { foo: 'bar' }, 3600);

// Get
const value = await redis.get<{ foo: string }>('key');

// Delete
await redis.del('key');

// Check existence
const exists = await redis.exists('key');

// Increment/Decrement
await redis.incr('counter');
await redis.decr('counter');
```

### Cache Invalidation

```typescript
// Invalidate single key
await redis.remove('user:123');

// Soft delete (marks as deleted but keeps in cache)
await redis.remove('user:123', true);

// Invalidate by pattern
await redis.invalidatePattern('user:*');

// Flush all keys with prefix
await redis.flushPrefix();
```

### Statistics & Health

```typescript
// Get cache statistics
const stats = redis.getStats();
// { hits: 1234, misses: 56, hitRate: 0.956 }

// Reset statistics
redis.resetStats();

// Health check
const health = await redis.healthCheck();
// { ok: true, latency: 5 }
```

## Configuration Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `url` | `string` | required | Redis connection URL |
| `prefix` | `string` | `'entrolytics:'` | Key prefix for all operations |
| `defaultTTL` | `number` | `3600` | Default TTL in seconds |

## License

MIT - Entrolytics
