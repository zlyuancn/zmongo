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
    c    *Client
    Coll *mongo.Collection
}

func (m *Collection) Aggregate(pipeline interface{}, opts ...*options.AggregateOptions) (*mongo.Cursor, error) {
    ctx, cancel := context.WithTimeout(context.Background(), m.c.DoTimeout)
    defer cancel()

    return m.Coll.Aggregate(ctx, pipeline, opts...)
}

func (m *Collection) BulkWrite(models []mongo.WriteModel, opts ...*options.BulkWriteOptions) (*mongo.BulkWriteResult, error) {
    ctx, cancel := context.WithTimeout(context.Background(), m.c.DoTimeout)
    defer cancel()

    return m.Coll.BulkWrite(ctx, models, opts...)
}

func (m *Collection) DeleteOne(filter interface{}, opts ...*options.DeleteOptions) (*mongo.DeleteResult, error) {
    ctx, cancel := context.WithTimeout(context.Background(), m.c.DoTimeout)
    defer cancel()

    return m.Coll.DeleteOne(ctx, filter, opts...)
}

func (m *Collection) DeleteMany(filter interface{}, opts ...*options.DeleteOptions) (*mongo.DeleteResult, error) {
    ctx, cancel := context.WithTimeout(context.Background(), m.c.DoTimeout)
    defer cancel()

    return m.Coll.DeleteMany(ctx, filter, opts...)
}

func (m *Collection) Find(filter interface{}, opts ...*options.FindOptions) (*mongo.Cursor, error) {
    ctx, cancel := context.WithTimeout(context.Background(), m.c.DoTimeout)
    defer cancel()

    return m.Coll.Find(ctx, filter, opts...)
}

func (m *Collection) FindOne(filter interface{}, opts ...*options.FindOneOptions) *mongo.SingleResult {
    ctx, cancel := context.WithTimeout(context.Background(), m.c.DoTimeout)
    defer cancel()

    return m.Coll.FindOne(ctx, filter, opts...)
}

func (m *Collection) FindOneAndDelete(filter interface{}, opts ...*options.FindOneAndDeleteOptions) *mongo.SingleResult {
    ctx, cancel := context.WithTimeout(context.Background(), m.c.DoTimeout)
    defer cancel()

    return m.Coll.FindOneAndDelete(ctx, filter, opts...)
}

func (m *Collection) FindOneAndReplace(filter, replacement interface{}, opts ...*options.FindOneAndReplaceOptions) *mongo.SingleResult {
    ctx, cancel := context.WithTimeout(context.Background(), m.c.DoTimeout)
    defer cancel()

    return m.Coll.FindOneAndReplace(ctx, filter, replacement, opts...)
}

func (m *Collection) FindOneAndUpdate(filter, update interface{}, opts ...*options.FindOneAndUpdateOptions) *mongo.SingleResult {
    ctx, cancel := context.WithTimeout(context.Background(), m.c.DoTimeout)
    defer cancel()

    return m.Coll.FindOneAndUpdate(ctx, filter, update, opts...)
}

func (m *Collection) InsertOne(document interface{}, opts ...*options.InsertOneOptions) (*mongo.InsertOneResult, error) {
    ctx, cancel := context.WithTimeout(context.Background(), m.c.DoTimeout)
    defer cancel()

    return m.Coll.InsertOne(ctx, document, opts...)
}

func (m *Collection) InsertMany(document []interface{}, opts ...*options.InsertManyOptions) (*mongo.InsertManyResult, error) {
    ctx, cancel := context.WithTimeout(context.Background(), m.c.DoTimeout)
    defer cancel()

    return m.Coll.InsertMany(ctx, document, opts...)
}

func (m *Collection) ReplaceOne(filter, replacement interface{}, opts ...*options.ReplaceOptions) (*mongo.UpdateResult, error) {
    ctx, cancel := context.WithTimeout(context.Background(), m.c.DoTimeout)
    defer cancel()

    return m.Coll.ReplaceOne(ctx, filter, replacement, opts...)
}

func (m *Collection) UpdateOne(filter, update interface{}, opts ...*options.UpdateOptions) (*mongo.UpdateResult, error) {
    ctx, cancel := context.WithTimeout(context.Background(), m.c.DoTimeout)
    defer cancel()

    return m.Coll.UpdateOne(ctx, filter, update, opts...)
}

func (m *Collection) UpdateMany(filter, update interface{}, opts ...*options.UpdateOptions) (*mongo.UpdateResult, error) {
    ctx, cancel := context.WithTimeout(context.Background(), m.c.DoTimeout)
    defer cancel()

    return m.Coll.UpdateMany(ctx, filter, update, opts...)
}
