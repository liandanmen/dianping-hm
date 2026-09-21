-- ARGV[1]: voucherId, ARGV[2]: userId, ARGV[3]: orderId
local voucherId = ARGV[1]
local userId = ARGV[2]
local orderId = ARGV[3]

local stockKey = 'seckill:stock:' .. voucherId
local orderKey = 'seckill:order:' .. voucherId
local beginTimeKey = 'seckill:begin:' .. voucherId
local endTimeKey = 'seckill:end:' .. voucherId
local reservationKey = 'seckill:reservation:' .. orderId

-- 使用 Redis 服务器时间，避免多台应用服务器时间不一致。
local redisTime = redis.call('time')
local now = tonumber(redisTime[1]) * 1000 + math.floor(tonumber(redisTime[2]) / 1000)
local beginTime = tonumber(redis.call('get', beginTimeKey))
local endTime = tonumber(redis.call('get', endTimeKey))

if not beginTime or not endTime then
    return 5
end
if now < beginTime then
    return 3
end
if now > endTime then
    return 4
end

local stock = tonumber(redis.call('get', stockKey))
if not stock or stock <= 0 then
    return 1
end
if redis.call('sismember', orderKey, userId) == 1 then
    return 2
end

-- 预扣库存、记录购买资格和订单预留必须在同一个 Lua 脚本内完成。
redis.call('incrby', stockKey, -1)
redis.call('sadd', orderKey, userId)
redis.call('hset', reservationKey,
        'orderId', orderId,
        'voucherId', voucherId,
        'userId', userId,
        'status', 'PENDING')

return 0
