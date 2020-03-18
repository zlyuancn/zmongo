/*
-------------------------------------------------
   Author :       Zhang Fan
   date：         2020/1/31
   Description :
-------------------------------------------------
*/

package zmongo

import (
    "context"
    "io"

    "go.mongodb.org/mongo-driver/mongo"
)

type FindAllResult struct {
    c   *Client
    err error
    cur *mongo.Cursor
}

// 将所有数据解码到a中并关闭游标, 即使数据获取失败也会关闭游标
func (m *FindAllResult) Decode(a interface{}) error {
    ctx, cancel := context.WithTimeout(context.Background(), m.c.DoTimeout)
    defer cancel()

    return m.DecodeWithContext(ctx, a)
}

// 将所有数据解码到a中并关闭游标, 即使数据获取失败也会关闭游标
func (m *FindAllResult) DecodeWithContext(ctx context.Context, a interface{}) error {
    if m.err != nil {
        return m.err
    }
    defer m.Close()

    if err := m.cur.All(ctx, a); err != nil {
        return err
    }
    return nil
}

// 解码下一个数据到a然后关闭游标
func (m *FindAllResult) DecodeOne(a interface{}) error {
    ctx, cancel := context.WithTimeout(context.Background(), m.c.DoTimeout)
    defer cancel()

    return m.DecodeOneWithContext(ctx, a)
}

// 解码下一个数据到a然后关闭游标
func (m *FindAllResult) DecodeOneWithContext(ctx context.Context, a interface{}) error {
    if m.err != nil {
        return m.err
    }
    defer m.Close()

    if !m.cur.Next(ctx) {
        return io.EOF
    }

    return m.cur.Decode(a)
}

// 解码下一个数据到a
func (m *FindAllResult) Next(a interface{}) error {
    ctx, cancel := context.WithTimeout(context.Background(), m.c.DoTimeout)
    defer cancel()

    return m.NextWithContext(ctx, a)
}

// 解码下一个数据到a
func (m *FindAllResult) NextWithContext(ctx context.Context, a interface{}) error {
    if m.err != nil {
        return m.err
    }

    if !m.cur.Next(ctx) {
        return io.EOF
    }

    return m.cur.Decode(a)
}

// 获取错误
func (m *FindAllResult) Err() error {
    return m.err
}

// 关闭游标
func (m *FindAllResult) Close() error {
    if m.err != nil {
        return m.err
    }

    ctx, cancel := context.WithTimeout(context.Background(), m.c.DoTimeout)
    defer cancel()

    return m.cur.Close(ctx)
}

// 关闭游标
func (m *FindAllResult) CloseWithContext(ctx context.Context) error {
    if m.err != nil {
        return m.err
    }

    return m.cur.Close(ctx)
}
