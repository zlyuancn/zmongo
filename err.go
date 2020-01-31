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
)

var (
    ErrNoMatch         = errors.New("No match")
    ErrNoMatchOrInsert = errors.New("No match or insert")
    ErrNoReplacement   = ErrNoMatchOrInsert
    ErrNoUpdate        = ErrNoMatchOrInsert
    ErrNoDelete        = ErrNoMatch
)
