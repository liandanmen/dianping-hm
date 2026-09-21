-- ARGV[1]: orderId, ARGV[2]: voucherId, ARGV[3]: userId
local orderId = ARGV[1]
local voucherId = ARGV[2]
local userId = ARGV[3]

local stockKey = 'seckill:stock:' .. voucherId
local orderKey = 'seckill:order:' .. voucherId
local reservationKey = 'seckill:reservation:' .. orderId
local compensatedKey = 'seckill:compensated:' .. orderId

if redis.call('hget', reservationKey, 'status') == 'CREATED' then
    return 2
end

-- SET NX 保证同一个订单只能进入一次补偿逻辑。
local firstCompensation = redis.call('set', compensatedKey, '1', 'NX', 'EX', 604800)
if not firstCompensation then
    return 0
end

-- 只有确实移除了购买资格才恢复库存，进一步防止库存被重复增加。
local removed = redis.call('srem', orderKey, userId)
if removed == 1 then
    redis.call('incrby', stockKey, 1)
end

redis.call('hset', reservationKey, 'status', 'COMPENSATED')
redis.call('expire', reservationKey, 604800)
return removed
