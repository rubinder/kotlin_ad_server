-- KEYS[1] = the winhistory:{userId} sorted-set key
-- ARGV[1] = score (timestamp, ms)
-- ARGV[2] = member ("{campaignId}:{category}")
-- ARGV[3] = trim threshold (drop entries with score < threshold)
-- Returns: the size of the zset after trim
redis.call('ZADD', KEYS[1], tonumber(ARGV[1]), ARGV[2])
redis.call('ZREMRANGEBYSCORE', KEYS[1], '-inf', tonumber(ARGV[3]))
return redis.call('ZCARD', KEYS[1])
