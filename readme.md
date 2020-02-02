# 基于官方驱动二次封装的mongo链接库, 使用更简单, 逻辑更清晰

---

# 获得

`go get -u github.com/zlyuancn/zmongo`

# 特性

+ 使用简单, 一行代码增删改查
+ 代码逻辑更清晰
+ 方法和参数完全和官方客户端一致(不需要传入context)
+ 可以透过zmongo直接调用官方客户端的方法
+ 更多的官方未实现的方法

# 说明

## 我一开始使用的[mgo](https://github.com/go-mgo/mgo)开发, 但是它好久都不更新了, 正好出现了[官方客户端](https://github.com/mongodb/mongo-go-driver). 但是我发现官方客户端虽然功能更全面但是使用相当繁琐, 开发时整体代码逻辑不是很好看, 部分操作结果除了检查err之外还需要检查resp, 所以zmongo诞生了 

# 文档
[godoc](https://godoc.org/github.com/zlyuancn/zmongo)

# 开始
```go
    db, err := zmongo.New(&zmongo.Config{
        Address:     []string{"127.0.0.1:27017"},
        DBName:      "test",
        DialTimeout: 0,
        DoTimeout:   0,
    })
    if err != nil {
        panic(err)
    }
    defer db.Close()
```
