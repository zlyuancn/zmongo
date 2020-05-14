/*
-------------------------------------------------
   Author :       Zhang Fan
   date：         2020/1/31
   Description :
-------------------------------------------------
*/

package zmongo

import (
    "errors"

    "go.mongodb.org/mongo-driver/mongo"
)

var (
    // 没有匹配
    ErrNoMatch = errors.New("No match")
    // 没有匹配或插入
    ErrNoMatchOrInsert = errors.New("No match or insert")
    // 没有替换
    ErrNoReplacement = ErrNoMatchOrInsert
    // 没有更新
    ErrNoUpdate = ErrNoMatchOrInsert
    // 没有删除
    ErrNoDelete = ErrNoMatch
    // 没有文档
    ErrNoDocuments = mongo.ErrNoDocuments
)
