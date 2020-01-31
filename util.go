/*
-------------------------------------------------
   Author :       Zhang Fan
   date：         2020/1/31
   Description :
-------------------------------------------------
*/

package zmongo

import (
    "strings"

    "go.mongodb.org/mongo-driver/bson"
)

func MakeSortBson(fields ...string) bson.D {
    var order bson.D
    for _, field := range fields {
        n := 1
        var kind string
        if field != "" {
            if field[0] == '$' {
                if c := strings.Index(field, ":"); c > 1 && c < len(field)-1 {
                    kind = field[1:c]
                    field = field[c+1:]
                }
            }
            switch field[0] {
            case '+':
                field = field[1:]
            case '-':
                n = -1
                field = field[1:]
            }
        }
        if field == "" {
            panic("Sort: empty field name")
        }
        if kind == "textScore" {
            order = append(order, bson.E{field, bson.M{"$meta": kind}})
        } else {
            order = append(order, bson.E{field, n})
        }
    }
    return order
}
