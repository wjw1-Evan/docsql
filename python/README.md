# DocSQL Python Driver

DocSQL 数据库的 Python 驱动:DB-API 2.0(PEP 249),纯标准库实现,直接对话
DocSQL v1 二进制线协议。参数在**服务端**绑定(REQ_PREPARE/REQ_EXECUTE),
绑定值永远无法改变语句文本 —— 注入面与 .NET/EF 驱动同一套关闭机制。

## 安装

```bash
pip install .            # 仓库 python/ 目录
pip install ".[crypto]"  # 可选:DOCSQL_KEY 帧加密(推荐新部署用 TLS)
```

## 快速开始

```python
import docsql

with docsql.connect(host="127.0.0.1", port=7600, token="...") as conn:
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO users (id, name, balance) VALUES (?, ?, ?)",
            (1, "alice", __import__("decimal").Decimal("9.99")),
        )
        cur.execute("SELECT id, name, balance FROM users WHERE balance > ?", ("1.00",))
        for row in cur.fetchall():
            print(row)  # (1, 'alice', Decimal('9.99'))
```

认证三选一:`token=`(节点客户端令牌)、`user=`+`password=`(数据库用户,
优先于 token)、都不给(匿名,服务端未配置凭据时可用)。

## 类型映射

| DocSQL | Python | 线协议形态 |
|---|---|---|
| INT | `int` | JSON 数字 |
| FLOAT | `float` | JSON 数字;非有限走 `{"$float":"NaN"/"inf"/"-inf"}` |
| DECIMAL | `decimal.Decimal` | `{"$dec":"<精确文本>"}` —— 不经过 IEEE double |
| TIMESTAMP | `datetime.datetime`(UTC) | `{"$ts":<毫秒>}` |
| BLOB | `bytes` | `{"$bytes":[ints]}` |
| TEXT | `str` | JSON 字符串 |
| BOOL / NULL | `bool` / `None` | `true/false` / `null` |
| JSON 文档 | `dict`/`list`(读) | 原生 JSON;绑定时序列化为 JSON 文本 |

超出 int64 的 Python 整数自动走 `$dec` 精确路径(裸 JSON 数字会退化为
IEEE double,WHERE 匹配会失配)。

## 事务

`autocommit` **默认 True**(偏离 DB-API 的隐式事务默认):DocSQL 是单写者
网络数据库,进程空闲时挂着隐式事务会把整条写路径停住。设
`autocommit=False` 后驱动按 psycopg2 风格在每个块的第一条语句前发
BEGIN,`commit()`/`rollback()` 结束块 —— 此模式不要自己写 BEGIN。
DDL 也在事务内(回滚会撤销建表)。

## 原生 TLS

```python
conn = docsql.connect(
    host="db.internal", port=7600, token="...",
    tls=True, tls_ca="/certs/ca.pem",           # 缺省 = 只加密不验证(自签形态)
    tls_hostname="db.internal",                 # 按 IP 连接而证书签给域名时用
)
```

## pub/sub 订阅

```python
sub = docsql.Subscriber(
    on_message=lambda m: print(m["channel"], m["id"], m["payload"]),
    host="127.0.0.1", port=7600, token="...",
)
sub.subscribe("news")              # from_="latest"|"earliest"|<id>
...
sub.close()
```

订阅使用专用连接 + 专职读线程(订阅连接是流式推送,不能复用一问一答
连接)。断线自动重连并**从每频道最后收到的 id 之后续传**(服务器语义
`id > from`,不丢不重);glob 模式订阅(`psubscribe`)重连后从 latest。

## 错误层级

DB-API 标准:`Error` → `InterfaceError`(传输层,连接作废)/
`DatabaseError`(→ `DataError` / `OperationalError` / `IntegrityError` /
`InternalError` / `ProgrammingError` / `NotSupportedError`)。映射基于
服务端错误文本的启发式,未识别的文本抛 `DatabaseError`。

## 运行测试

```bash
cargo build -p docsql-server        # 仓库根;测试会启动 debug 二进制
pip install pytest cryptography
python -m pytest python/tests -q    # 29 用例
```

测试自备自签证书(`tests/fixtures/`,SAN 含 localhost 与 127.0.0.1),
覆盖 TLS 证书验证、只加密不验证、明文客户端被拒等路径。
