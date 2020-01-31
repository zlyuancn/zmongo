/*
-------------------------------------------------
   Author :       Zhang Fan
   date：         2019/12/29
   Description :
-------------------------------------------------
*/

package zmongo

import (
    "context"

    "go.mongodb.org/mongo-driver/mongo"
)

type Cursor struct {
    c *Client
    *mongo.Cursor
}

func makeCursor(c *Client, cur *mongo.Cursor) *Cursor {
    return &Cursor{
        c:      c,
        Cursor: cur,
    }
}

// 下一个文档
func (m *Cursor) Next() bool {
    ctx, cancel := context.WithTimeout(context.Background(), m.c.DoTimeout)
    defer cancel()

    return m.Cursor.Next(ctx)
}

// 下一个文档
func (m *Cursor) TryNext() bool {
    ctx, cancel := context.WithTimeout(context.Background(), m.c.DoTimeout)
    defer cancel()

    return m.Cursor.TryNext(ctx)
}

// 关闭游标
func (m *Cursor) Close() error {
    ctx, cancel := context.WithTimeout(context.Background(), m.c.DoTimeout)
    defer cancel()

    return m.Cursor.Close(ctx)
}

// 获取所有文档并解码到results
func (m *Cursor) All(results interface{}) error {
    ctx, cancel := context.WithTimeout(context.Background(), m.c.DoTimeout)
    defer cancel()

    return m.Cursor.All(ctx, results)
}
