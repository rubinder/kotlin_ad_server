-- KEYS[1] = the freq:{userId}:{campaignId} key
-- ARGV[1] = increment amount (integer)
-- ARGV[2] = TTL in seconds
-- Returns: the new counter value
local newval = redis.call('INCRBY', KEYS[1], tonumber(ARGV[1]))
redis.call('EXPIRE', KEYS[1], tonumber(ARGV[2]))
return newval
