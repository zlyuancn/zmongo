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

    "go.mongodb.org/mongo-driver/bson"
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

func (m *Collection) AggregateAll(pipeline interface{}, opts ...*options.AggregateOptions) *FindAllResult {
    ctx, cancel := context.WithTimeout(context.Background(), m.c.DoTimeout)
    defer cancel()

    return m.AggregateAllWithContext(ctx, pipeline, opts...)
}

func (m *Collection) AggregateAllWithContext(ctx context.Context, pipeline interface{}, opts ...*options.AggregateOptions) *FindAllResult {
    cur, err := m.Collection.Aggregate(ctx, pipeline, opts...)

    return &FindAllResult{c: m.c, err: err, cur: cur}
}

func (m *Collection) BulkWrite(models []mongo.WriteModel, opts ...*options.BulkWriteOptions) (*mongo.BulkWriteResult, error) {
    ctx, cancel := context.WithTimeout(context.Background(), m.c.DoTimeout)
    defer cancel()

    return m.Collection.BulkWrite(ctx, models, opts...)
}

func (m *Collection) CountDocuments(filter interface{}, opts ...*options.CountOptions) (int64, error) {
    ctx, cancel := context.WithTimeout(context.Background(), m.c.DoTimeout)
    defer cancel()

    return m.Collection.CountDocuments(ctx, filter, opts...)
}

func (m *Collection) DeleteOne(filter interface{}, opts ...*options.DeleteOptions) (*mongo.DeleteResult, error) {
    ctx, cancel := context.WithTimeout(context.Background(), m.c.DoTimeout)
    defer cancel()

    return m.Collection.DeleteOne(ctx, filter, opts...)
}

func (m *Collection) Drop() error {
    ctx, cancel := context.WithTimeout(context.Background(), m.c.DoTimeout)
    defer cancel()

    return m.Collection.Drop(ctx)
}

func (m *Collection) Distinct(fieldName string, filter interface{}, opts ...*options.DistinctOptions) ([]interface{}, error) {
    ctx, cancel := context.WithTimeout(context.Background(), m.c.DoTimeout)
    defer cancel()

    return m.Collection.Distinct(ctx, fieldName, filter, opts...)
}

func (m *Collection) EstimatedDocumentCount(opts ...*options.EstimatedDocumentCountOptions) (int64, error) {
    ctx, cancel := context.WithTimeout(context.Background(), m.c.DoTimeout)
    defer cancel()

    return m.Collection.EstimatedDocumentCount(ctx, opts...)
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

func (m *Collection) FindAll(filter interface{}, opts ...*options.FindOptions) *FindAllResult {
    ctx, cancel := context.WithTimeout(context.Background(), m.c.DoTimeout)
    defer cancel()

    return m.FindAllWithContext(ctx, filter, opts...)
}

func (m *Collection) FindAllWithContext(ctx context.Context, filter interface{}, opts ...*options.FindOptions) *FindAllResult {
    cur, err := m.Collection.Find(ctx, filter, opts...)

    return &FindAllResult{c: m.c, err: err, cur: cur}
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

func (m *Collection) MustDeleteOne(filter interface{}, opts ...*options.DeleteOptions) error {
    ctx, cancel := context.WithTimeout(context.Background(), m.c.DoTimeout)
    defer cancel()

    return m.MustDeleteOneWithContext(ctx, filter, opts...)
}

func (m *Collection) MustDeleteOneWithContext(ctx context.Context, filter interface{}, opts ...*options.DeleteOptions) error {
    resp, err := m.Collection.DeleteOne(ctx, filter, opts...)
    if err != nil {
        return err
    }
    if resp.DeletedCount == 0 {
        return ErrNoDelete
    }

    return nil
}

func (m *Collection) MustReplaceOne(filter, replacement interface{}, opts ...*options.ReplaceOptions) error {
    ctx, cancel := context.WithTimeout(context.Background(), m.c.DoTimeout)
    defer cancel()

    return m.MustReplaceOneWithContext(ctx, filter, replacement, opts...)
}

func (m *Collection) MustReplaceOneWithContext(ctx context.Context, filter, replacement interface{}, opts ...*options.ReplaceOptions) error {
    resp, err := m.Collection.ReplaceOne(ctx, filter, replacement, opts...)
    if err != nil {
        return err
    }
    if resp.MatchedCount+resp.UpsertedCount == 0 {
        return ErrNoReplacement
    }

    return nil
}

func (m *Collection) MustUpdateOne(filter, update interface{}, opts ...*options.UpdateOptions) error {
    ctx, cancel := context.WithTimeout(context.Background(), m.c.DoTimeout)
    defer cancel()

    return m.MustUpdateOneWithContext(ctx, filter, update, opts...)
}

func (m *Collection) MustUpdateOneWithContext(ctx context.Context, filter, update interface{}, opts ...*options.UpdateOptions) error {
    resp, err := m.Collection.UpdateOne(ctx, filter, update, opts...)
    if err != nil {
        return err
    }
    if resp.MatchedCount+resp.UpsertedCount == 0 {
        return ErrNoUpdate
    }

    return nil
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

func (m *Collection) Upsert(id string, doc interface{}) error {
    ctx, cancel := context.WithTimeout(context.Background(), m.c.DoTimeout)
    defer cancel()

    return m.UpsertWithContext(ctx, id, doc)
}

func (m *Collection) UpsertWithContext(ctx context.Context, id string, doc interface{}) error {
    _, err := m.Collection.ReplaceOne(ctx, bson.M{"_id": id}, doc, options.Replace().SetUpsert(true))
    return err
}

func (m *Collection) Watch(pipeline interface{}, opts ...*options.ChangeStreamOptions) (*mongo.ChangeStream, error) {
    ctx, cancel := context.WithTimeout(context.Background(), m.c.DoTimeout)
    defer cancel()

    return m.Collection.Watch(ctx, pipeline, opts...)
}
