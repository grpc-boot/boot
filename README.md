# boot基础框架

## 1.mysql

### 1.1 使用group

> 初始化group

```go
    option := mysql.GroupOption{
		RetryInterval: 60,
		Masters:       []mysql.PoolOption{
			{
				Dsn:`{userName}:{password}@tcp({host}:{port})/{dbName}?timeout=5s&readTimeout=6s`,
				MaxConnLifetime: 600,
				MaxOpenConns: 50,
				MaxIdleConns: 10,
			},
		},
		Slaves:        []mysql.PoolOption{
			{
				Dsn:`{userName}:{password}@tcp({host}:{port})/{dbName}?timeout=5s&readTimeout=6s`,
				MaxConnLifetime: 600,
				MaxOpenConns: 50,
				MaxIdleConns: 10,
			},
		},
	}
	group = mysql.NewGroup(&option)
```

> 从yaml配置文件加载group

* yaml配置文件内容

```yaml
boot:
    retryInterval: 60
    masters:
      - dsn: root:123456@tcp(127.0.0.1:3306)/test?timeout=5s&readTimeout=6s
        maxConnLifetime: 600
        maxOpenConns: 50
        maxIdleConns: 10

      - dsn: root:123456@tcp(127.0.0.2:3306)/test?timeout=5s&readTimeout=6s
        maxConnLifetime: 600
        maxOpenConns: 50
        maxIdleConns: 10

    slaves:
      - dsn: root:123456@tcp(127.0.0.3:3306)/test?timeout=5s&readTimeout=6s
        maxConnLifetime: 600
        maxOpenConns: 50
        maxIdleConns: 10
```

* 初始化group

```go
    type Config struct {
        Boot mysql.GroupOption `yaml:"boot" json:"boot"`
    }

    config = &Config{}
	//加载配置
	boot.Yaml("app.yml", config)
	
	//初始化mysqlGroup
	group = mysql.NewGroup(&config.Boot)
```
