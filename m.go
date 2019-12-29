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

const DefaultDoTimeout = time.Second * 5

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

func New(conf *Config) (*Client, error) {
    opt := &options.ClientOptions{
        Hosts:          conf.Address,
        MaxPoolSize:    &conf.PoolSize,
        ConnectTimeout: &conf.DialTimeout,
    }
    if conf.UserName != "" {
        opt.Auth = &options.Credential{
            AuthSource: conf.DBName,
            Username:   conf.UserName,
            Password:   conf.Password,
        }
    }

    ctx, cancel := context.WithTimeout(context.Background(), conf.DialTimeout)
    defer cancel()

    client, err := mongo.Connect(ctx, opt)
    if err != nil {
        return nil, err
    }

    m := &Client{
        Client: client,
        Config: *conf,
    }
    if m.DoTimeout == 0 {
        m.DoTimeout = DefaultDoTimeout
    }

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

func (m *Client) Ping(rp *readpref.ReadPref) error {
    ctx, cancel := context.WithTimeout(context.Background(), m.DialTimeout)
    defer cancel()

    return m.Client.Ping(ctx, rp)
}
