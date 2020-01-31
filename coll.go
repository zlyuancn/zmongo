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

// 聚合查询
func (m *Collection) Aggregate(pipeline interface{}, opts ...*options.AggregateOptions) (*Cursor, error) {
    ctx, cancel := context.WithTimeout(context.Background(), m.c.DoTimeout)
    defer cancel()

    cur, err := m.Collection.Aggregate(ctx, pipeline, opts...)
    if err != nil {
        return nil, err
    }

    return makeCursor(m.c, cur), nil
}

// 聚合查询
func (m *Collection) AggregateAll(pipeline interface{}, opts ...*options.AggregateOptions) *FindAllResult {
    ctx, cancel := context.WithTimeout(context.Background(), m.c.DoTimeout)
    defer cancel()

    return m.AggregateAllWithContext(ctx, pipeline, opts...)
}

// 聚合查询
func (m *Collection) AggregateAllWithContext(ctx context.Context, pipeline interface{}, opts ...*options.AggregateOptions) *FindAllResult {
    cur, err := m.Collection.Aggregate(ctx, pipeline, opts...)

    return &FindAllResult{c: m.c, err: err, cur: cur}
}

// 批量操作
func (m *Collection) BulkWrite(models []mongo.WriteModel, opts ...*options.BulkWriteOptions) (*mongo.BulkWriteResult, error) {
    ctx, cancel := context.WithTimeout(context.Background(), m.c.DoTimeout)
    defer cancel()

    return m.Collection.BulkWrite(ctx, models, opts...)
}

// 获取文档数量
func (m *Collection) CountDocuments(filter interface{}, opts ...*options.CountOptions) (int64, error) {
    ctx, cancel := context.WithTimeout(context.Background(), m.c.DoTimeout)
    defer cancel()

    return m.Collection.CountDocuments(ctx, filter, opts...)
}

// 删除一个文档
func (m *Collection) DeleteOne(filter interface{}, opts ...*options.DeleteOptions) (*mongo.DeleteResult, error) {
    ctx, cancel := context.WithTimeout(context.Background(), m.c.DoTimeout)
    defer cancel()

    return m.Collection.DeleteOne(ctx, filter, opts...)
}

// 删除多个文档
func (m *Collection) DeleteMany(filter interface{}, opts ...*options.DeleteOptions) (*mongo.DeleteResult, error) {
    ctx, cancel := context.WithTimeout(context.Background(), m.c.DoTimeout)
    defer cancel()

    return m.Collection.DeleteMany(ctx, filter, opts...)
}

// 删除当前集合
func (m *Collection) Drop() error {
    ctx, cancel := context.WithTimeout(context.Background(), m.c.DoTimeout)
    defer cancel()

    return m.Collection.Drop(ctx)
}

//
func (m *Collection) Distinct(fieldName string, filter interface{}, opts ...*options.DistinctOptions) ([]interface{}, error) {
    ctx, cancel := context.WithTimeout(context.Background(), m.c.DoTimeout)
    defer cancel()

    return m.Collection.Distinct(ctx, fieldName, filter, opts...)
}

// 获取当前集合总数
func (m *Collection) EstimatedDocumentCount(opts ...*options.EstimatedDocumentCountOptions) (int64, error) {
    ctx, cancel := context.WithTimeout(context.Background(), m.c.DoTimeout)
    defer cancel()

    return m.Collection.EstimatedDocumentCount(ctx, opts...)
}

// 查找
func (m *Collection) Find(filter interface{}, opts ...*options.FindOptions) (*Cursor, error) {
    ctx, cancel := context.WithTimeout(context.Background(), m.c.DoTimeout)
    defer cancel()

    cur, err := m.Collection.Find(ctx, filter, opts...)
    if err != nil {
        return nil, err
    }

    return makeCursor(m.c, cur), nil
}

// 查找
func (m *Collection) FindAll(filter interface{}, opts ...*options.FindOptions) *FindAllResult {
    ctx, cancel := context.WithTimeout(context.Background(), m.c.DoTimeout)
    defer cancel()

    return m.FindAllWithContext(ctx, filter, opts...)
}

// 查找
func (m *Collection) FindAllWithContext(ctx context.Context, filter interface{}, opts ...*options.FindOptions) *FindAllResult {
    cur, err := m.Collection.Find(ctx, filter, opts...)

    return &FindAllResult{c: m.c, err: err, cur: cur}
}

// 查找一个文档
func (m *Collection) FindOne(filter interface{}, opts ...*options.FindOneOptions) *mongo.SingleResult {
    ctx, cancel := context.WithTimeout(context.Background(), m.c.DoTimeout)
    defer cancel()

    return m.Collection.FindOne(ctx, filter, opts...)
}

// 查找一个文档并删除
func (m *Collection) FindOneAndDelete(filter interface{}, opts ...*options.FindOneAndDeleteOptions) *mongo.SingleResult {
    ctx, cancel := context.WithTimeout(context.Background(), m.c.DoTimeout)
    defer cancel()

    return m.Collection.FindOneAndDelete(ctx, filter, opts...)
}

// 查找一个文档并替换
func (m *Collection) FindOneAndReplace(filter, replacement interface{}, opts ...*options.FindOneAndReplaceOptions) *mongo.SingleResult {
    ctx, cancel := context.WithTimeout(context.Background(), m.c.DoTimeout)
    defer cancel()

    return m.Collection.FindOneAndReplace(ctx, filter, replacement, opts...)
}

// 查找一个文档并更新
func (m *Collection) FindOneAndUpdate(filter, update interface{}, opts ...*options.FindOneAndUpdateOptions) *mongo.SingleResult {
    ctx, cancel := context.WithTimeout(context.Background(), m.c.DoTimeout)
    defer cancel()

    return m.Collection.FindOneAndUpdate(ctx, filter, update, opts...)
}

// 插入一个文档
func (m *Collection) InsertOne(document interface{}, opts ...*options.InsertOneOptions) (*mongo.InsertOneResult, error) {
    ctx, cancel := context.WithTimeout(context.Background(), m.c.DoTimeout)
    defer cancel()

    return m.Collection.InsertOne(ctx, document, opts...)
}

// 插入多个文档
func (m *Collection) InsertMany(document []interface{}, opts ...*options.InsertManyOptions) (*mongo.InsertManyResult, error) {
    ctx, cancel := context.WithTimeout(context.Background(), m.c.DoTimeout)
    defer cancel()

    return m.Collection.InsertMany(ctx, document, opts...)
}

// 必须删除一个文档
func (m *Collection) MustDeleteOne(filter interface{}, opts ...*options.DeleteOptions) error {
    ctx, cancel := context.WithTimeout(context.Background(), m.c.DoTimeout)
    defer cancel()

    return m.MustDeleteOneWithContext(ctx, filter, opts...)
}

// 必须删除一个文档
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

// 必须替换一个文档
func (m *Collection) MustReplaceOne(filter, replacement interface{}, opts ...*options.ReplaceOptions) error {
    ctx, cancel := context.WithTimeout(context.Background(), m.c.DoTimeout)
    defer cancel()

    return m.MustReplaceOneWithContext(ctx, filter, replacement, opts...)
}

// 必须替换一个文档
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

// 必须更新一个文档
func (m *Collection) MustUpdateOne(filter, update interface{}, opts ...*options.UpdateOptions) error {
    ctx, cancel := context.WithTimeout(context.Background(), m.c.DoTimeout)
    defer cancel()

    return m.MustUpdateOneWithContext(ctx, filter, update, opts...)
}

// 必须更新一个文档
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

// 替换一个文档
func (m *Collection) ReplaceOne(filter, replacement interface{}, opts ...*options.ReplaceOptions) (*mongo.UpdateResult, error) {
    ctx, cancel := context.WithTimeout(context.Background(), m.c.DoTimeout)
    defer cancel()

    return m.Collection.ReplaceOne(ctx, filter, replacement, opts...)
}

// 更新一个文档
func (m *Collection) UpdateOne(filter, update interface{}, opts ...*options.UpdateOptions) (*mongo.UpdateResult, error) {
    ctx, cancel := context.WithTimeout(context.Background(), m.c.DoTimeout)
    defer cancel()

    return m.Collection.UpdateOne(ctx, filter, update, opts...)
}

// 更新多个文档
func (m *Collection) UpdateMany(filter, update interface{}, opts ...*options.UpdateOptions) (*mongo.UpdateResult, error) {
    ctx, cancel := context.WithTimeout(context.Background(), m.c.DoTimeout)
    defer cancel()

    return m.Collection.UpdateMany(ctx, filter, update, opts...)
}

// 替换或插入一个文档
func (m *Collection) Upsert(id string, doc interface{}) error {
    ctx, cancel := context.WithTimeout(context.Background(), m.c.DoTimeout)
    defer cancel()

    return m.UpsertWithContext(ctx, id, doc)
}

// 替换或插入一个文档
func (m *Collection) UpsertWithContext(ctx context.Context, id string, doc interface{}) error {
    _, err := m.Collection.ReplaceOne(ctx, bson.M{"_id": id}, doc, options.Replace().SetUpsert(true))
    return err
}

// 监视
func (m *Collection) Watch(pipeline interface{}, opts ...*options.ChangeStreamOptions) (*mongo.ChangeStream, error) {
    ctx, cancel := context.WithTimeout(context.Background(), m.c.DoTimeout)
    defer cancel()

    return m.Collection.Watch(ctx, pipeline, opts...)
}
