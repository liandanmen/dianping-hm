# 点评项目
基于Spring Boot的本地生活点评系统，包含短信登录、商铺查询、缓存优化、优惠券秒杀和异步下单等功能。
> 本项目基于学习项目进行二次开发，主要用于Java后端技术学习和项目实践。

## 技术栈
- Spring Boot
- MyBatis-Plus
- MySQL
- Redis
- RabbitMQ
- Maven
- Git
- Docker
- 
## 核心功能

- 短信验证码登录
- 用户登录状态管理
- 商铺查询与缓存
- 优惠券秒杀
- 一人一单
- 异步创建订单
- 订单状态管理

## 项目亮点
1. 使用 Redis 保存短信验证码和用户登录状态，减少数据库访问。
2. 针对商铺缓存击穿问题，使用互斥锁和逻辑过期方案进行优化。
3. 使用 Redis Lua 脚本原子完成库存校验、库存扣减和一人一单判断。
4. 使用全局唯一 ID 生成订单编号。
5. 使用数据库唯一索引防止重复创建订单。
6. 使用 RabbitMQ 异步处理秒杀订单，降低高并发场景下数据库压力。

## 项目启动

### 环境要求

- JDK 8+
- Maven 3.8+
- MySQL
- Redis
- RabbitMQ

### 配置步骤

1. 创建数据库 `hm_dianping`。
2. 执行 `sql` 目录下的数据库脚本。
3. 准备本地配置，并填写数据库、Redis、RabbitMQ 连接信息。
4. 启动 MySQL、Redis 和 RabbitMQ。
5. 执行以下命令启动项目：

```bash
mvn spring-boot:run

