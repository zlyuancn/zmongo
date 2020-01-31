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

    "go.mongodb.org/mongo-driver/mongo"
)

type FindAllResult struct {
    c   *Client
    err error
    cur *mongo.Cursor
}

func (m *FindAllResult) Decode(a interface{}) error {
    ctx, cancel := context.WithTimeout(context.Background(), m.c.DoTimeout)
    defer cancel()

    return m.DecodeWithContext(ctx, a)
}

func (m *FindAllResult) DecodeWithContext(ctx context.Context, a interface{}) error {
    if m.err != nil {
        return m.err
    }

    if err := m.cur.All(ctx, a); err != nil {
        return err
    }
    _ = m.Close()
    return nil
}

func (m *FindAllResult) Err() error {
    return m.err
}

func (m *FindAllResult) Close() error {
    if m.err != nil {
        return m.err
    }

    ctx, cancel := context.WithTimeout(context.Background(), m.c.DoTimeout)
    defer cancel()

    return m.cur.Close(ctx)
}

func (m *FindAllResult) CloseWithContext(ctx context.Context) error {
    if m.err != nil {
        return m.err
    }

    return m.cur.Close(ctx)
}
