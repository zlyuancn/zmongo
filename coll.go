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

    "go.mongodb.org/mongo-driver/mongo"
    "go.mongodb.org/mongo-driver/mongo/options"
)

type Collection struct {
    c *Client
    *mongo.Collection
}

func makeCollection(c *Client, coll *mongo.Collection) *Collection {
    return &Collection{
        c:          c,
        Collection: coll,
    }
}

func (m *Collection) Aggregate(pipeline interface{}, opts ...*options.AggregateOptions) (*Cursor, error) {
    ctx, cancel := context.WithTimeout(context.Background(), m.c.DoTimeout)
    defer cancel()

    cur, err := m.Collection.Aggregate(ctx, pipeline, opts...)
    if err != nil {
        return nil, err
    }

    return makeCursor(m.c, cur), nil
}

func (m *Collection) BulkWrite(models []mongo.WriteModel, opts ...*options.BulkWriteOptions) (*mongo.BulkWriteResult, error) {
    ctx, cancel := context.WithTimeout(context.Background(), m.c.DoTimeout)
    defer cancel()

    return m.Collection.BulkWrite(ctx, models, opts...)
}

func (m *Collection) DeleteOne(filter interface{}, opts ...*options.DeleteOptions) (*mongo.DeleteResult, error) {
    ctx, cancel := context.WithTimeout(context.Background(), m.c.DoTimeout)
    defer cancel()

    return m.Collection.DeleteOne(ctx, filter, opts...)
}

func (m *Collection) DeleteMany(filter interface{}, opts ...*options.DeleteOptions) (*mongo.DeleteResult, error) {
    ctx, cancel := context.WithTimeout(context.Background(), m.c.DoTimeout)
    defer cancel()

    return m.Collection.DeleteMany(ctx, filter, opts...)
}

func (m *Collection) Find(filter interface{}, opts ...*options.FindOptions) (*Cursor, error) {
    ctx, cancel := context.WithTimeout(context.Background(), m.c.DoTimeout)
    defer cancel()

    cur, err := m.Collection.Find(ctx, filter, opts...)
    if err != nil {
        return nil, err
    }

    return makeCursor(m.c, cur), nil
}

func (m *Collection) FindOne(filter interface{}, opts ...*options.FindOneOptions) *mongo.SingleResult {
    ctx, cancel := context.WithTimeout(context.Background(), m.c.DoTimeout)
    defer cancel()

    return m.Collection.FindOne(ctx, filter, opts...)
}

func (m *Collection) FindOneAndDelete(filter interface{}, opts ...*options.FindOneAndDeleteOptions) *mongo.SingleResult {
    ctx, cancel := context.WithTimeout(context.Background(), m.c.DoTimeout)
    defer cancel()

    return m.Collection.FindOneAndDelete(ctx, filter, opts...)
}

func (m *Collection) FindOneAndReplace(filter, replacement interface{}, opts ...*options.FindOneAndReplaceOptions) *mongo.SingleResult {
    ctx, cancel := context.WithTimeout(context.Background(), m.c.DoTimeout)
    defer cancel()

    return m.Collection.FindOneAndReplace(ctx, filter, replacement, opts...)
}

func (m *Collection) FindOneAndUpdate(filter, update interface{}, opts ...*options.FindOneAndUpdateOptions) *mongo.SingleResult {
    ctx, cancel := context.WithTimeout(context.Background(), m.c.DoTimeout)
    defer cancel()

    return m.Collection.FindOneAndUpdate(ctx, filter, update, opts...)
}

func (m *Collection) InsertOne(document interface{}, opts ...*options.InsertOneOptions) (*mongo.InsertOneResult, error) {
    ctx, cancel := context.WithTimeout(context.Background(), m.c.DoTimeout)
    defer cancel()

    return m.Collection.InsertOne(ctx, document, opts...)
}

func (m *Collection) InsertMany(document []interface{}, opts ...*options.InsertManyOptions) (*mongo.InsertManyResult, error) {
    ctx, cancel := context.WithTimeout(context.Background(), m.c.DoTimeout)
    defer cancel()

    return m.Collection.InsertMany(ctx, document, opts...)
}

func (m *Collection) ReplaceOne(filter, replacement interface{}, opts ...*options.ReplaceOptions) (*mongo.UpdateResult, error) {
    ctx, cancel := context.WithTimeout(context.Background(), m.c.DoTimeout)
    defer cancel()

    return m.Collection.ReplaceOne(ctx, filter, replacement, opts...)
}

func (m *Collection) UpdateOne(filter, update interface{}, opts ...*options.UpdateOptions) (*mongo.UpdateResult, error) {
    ctx, cancel := context.WithTimeout(context.Background(), m.c.DoTimeout)
    defer cancel()

    return m.Collection.UpdateOne(ctx, filter, update, opts...)
}

func (m *Collection) UpdateMany(filter, update interface{}, opts ...*options.UpdateOptions) (*mongo.UpdateResult, error) {
    ctx, cancel := context.WithTimeout(context.Background(), m.c.DoTimeout)
    defer cancel()

    return m.Collection.UpdateMany(ctx, filter, update, opts...)
}
