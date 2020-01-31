/*
-------------------------------------------------
   Author :       Zhang Fan
   date：         2019/12/28
   Description :
-------------------------------------------------
*/

package zmongo

import (
    "context"
    "time"

    "go.mongodb.org/mongo-driver/mongo"
    "go.mongodb.org/mongo-driver/mongo/options"
    "go.mongodb.org/mongo-driver/mongo/readpref"
)

const (
    // 默认连接超时
    DefaultDialTimeout = time.Second * 5
    // 默认操作超时
    DefaultDoTimeout = time.Second * 5
)

type Config struct {
    Address     []string      // 连接地址, 如: []string{"127.0.0.1:27017"}
    DBName      string        // 库名
    UserName    string        // 用户名
    Password    string        // 密码
    PoolSize    uint64        // 连接池的数量
    DialTimeout time.Duration // 连接超时(毫秒
    DoTimeout   time.Duration // 操作超时
}

type Client struct {
    *mongo.Client
    Config
}

// 创建一个客户端
func New(conf *Config) (*Client, error) {
    m := &Client{
        Config: *conf,
    }
    if m.DialTimeout == 0 {
        m.DialTimeout = DefaultDialTimeout
    }
    if m.DoTimeout == 0 {
        m.DoTimeout = DefaultDoTimeout
    }

    opt := &options.ClientOptions{
        Hosts:          m.Address,
        MaxPoolSize:    &m.PoolSize,
        ConnectTimeout: &m.DialTimeout,
    }
    if m.UserName != "" {
        opt.Auth = &options.Credential{
            AuthSource: m.DBName,
            Username:   m.UserName,
            Password:   m.Password,
        }
    }

    ctx, cancel := context.WithTimeout(context.Background(), m.DialTimeout)
    defer cancel()

    client, err := mongo.Connect(ctx, opt)
    if err != nil {
        return nil, err
    }

    m.Client = client
    return m, nil
}

// 返回一个文档集合
func (m *Client) Coll(database, collname string, opts ...*options.DatabaseOptions) *Collection {
    if database == "" {
        database = m.DBName
    }
    coll := m.Client.Database(database, opts...).Collection(collname)
    return makeCollection(m, coll)
}

// 关闭连接
func (m *Client) Close() error {
    ctx, cancel := context.WithTimeout(context.Background(), m.DialTimeout)
    defer cancel()

    return m.Client.Disconnect(ctx)
}

// ping
func (m *Client) Ping(rp *readpref.ReadPref) error {
    ctx, cancel := context.WithTimeout(context.Background(), m.DoTimeout)
    defer cancel()

    return m.Client.Ping(ctx, rp)
}
