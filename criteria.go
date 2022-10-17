package mongoext

import (
	"fmt"
	"github.com/go-fc/slice"
	"github.com/spf13/cast"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"net/url"
	"strings"
)

const (
	QueryIndex     = "index"
	QuerySize      = "size"
	QuerySort      = "sort"
	QuerySortArr   = "sort[]"
	QueryCondition = "condition"
	QueryField     = "field"
	QueryValue     = "value"

	CondEmpty = ""
	CondEq    = "eq"
	CondNe    = "ne"
	CondGt    = "gt"
	CondGte   = "gte"
	CondLt    = "lt"
	CondLte   = "lte"
	CondIn    = "in"
	CondNin   = "nin"
	CondRegex = "regex"
	CondLike  = "like"
)

var conditions = map[string]Operator{
	CondEmpty: OpEq,
	CondEq:    OpEq,
	CondNe:    OpNe,
	CondGt:    OpGt,
	CondGte:   OpGte,
	CondLt:    OpLt,
	CondLte:   OpLte,
	CondIn:    OpIn,
	CondNin:   OpNin,
	CondRegex: OpRegex,
	CondLike:  OpRegex,
}

type Criteria struct {
	Filter any
	Sort   any
	Index  int64
	Size   int64
}

func MustC(query, filterName string) Criteria {
	return MustGetCriteria(query, filterName)
}

// MustGetCriteria query example: index=20&size=20&sort[]=sku&sort[]=-amount&filters[0][0][field]=sku&filters[0][0][value]=WSH%2529%25&filters[0][0][condition]=like&filters[0][1][field]=sku&filters[0][1][value]=WP%2529%25&filters[0][1][condition]=like&filters[1][0][field]=price&filters[1][0][value]=40&filters[1][0][condition]=eq&filters[2][0][field]=price&filters[2][0][value]=49.99&filters[2][0][condition]=to
func MustGetCriteria(query, filterName string) Criteria {
	c, err := GetCriteria(query, filterName)
	if err != nil {
		panic(err)
	}
	return c
}

func GetC(query, filterName string) (Criteria, error) {
	return GetCriteria(query, filterName)
}

func GetCriteria(query, filterName string) (Criteria, error) {
	values, err := url.ParseQuery(query)
	if err != nil {
		return Criteria{Size: DefaultSize}, err
	}
	return C(values, filterName), nil
}

func C(values url.Values, filterName string) Criteria {
	return ToCriteria(values, filterName)
}

func ToCriteria(values url.Values, filterName string) Criteria {
	var s any

	index := cast.ToInt64(values.Get(QueryIndex))
	if index < 0 {
		index = 0
	}

	size := cast.ToInt64(values.Get(QuerySize))
	if size <= 0 {
		size = DefaultSize
	}

	if o, ok := values[QuerySortArr]; ok {
		s = order(o)
	} else if o, ok = values[QuerySort]; ok {
		s = order(o)
	}

	return Criteria{
		Filter: ToFilter(values, filterName).Build(),
		Sort:   s,
		Index:  index,
		Size:   size,
	}
}

func order(data []string) any {
	var o bson.D
	for _, item := range data {
		if len(item) > 0 {
			if '-' == item[0] {
				if len(item) > 1 {
					o = append(o, primitive.E{Key: item[1:], Value: -1})
				}
			} else {
				o = append(o, primitive.E{Key: item, Value: 1})
			}
		}
	}
	return o
}

func ToFilter(values url.Values, name string) Filter {
	filter, _ := Parse(values)[name]
	return filter
}

func Parse(values url.Values) map[string]Filter {
	tmp1 := make(map[string]struct{})
	tmp2 := make(map[string]map[string]*Logical)
	result := make(map[string]Filter)

	//        And Or
	//         i  j
	//         |  |
	// filters[0][0][condition]
	// filters[0][0][field]
	// filters[0][0][Value]
	for key := range values {
		if data := fields(key); len(data) == 4 {
			k := fmt.Sprintf("%s[%s][%s]", data[0], data[1], data[2])
			if _, ok := tmp1[k]; ok {
				continue
			}
			tmp1[k] = struct{}{}

			var field string
			var op Operator
			var ok bool
			if field = value(values, append(data[:3], QueryField)); field == "" {
				continue
			}
			if op, ok = conditions[value(values, append(data[:3], QueryCondition))]; !ok {
				continue
			}
			val := parse(value(values, append(data[:3], QueryValue)), op)

			if _, ok = tmp2[data[0]]; !ok {
				tmp2[data[0]] = map[string]*Logical{data[1]: {Op: OpOr}}
			} else if _, ok = tmp2[data[0]][data[1]]; !ok {
				tmp2[data[0]][data[1]] = &Logical{Op: OpOr}
			}

			var expr Expr
			if op == OpRegex {
				expr = Regex(field, val.(string), "i")
			} else {
				expr = &Field{Op: op, Name: field, Value: val}
			}
			tmp2[data[0]][data[1]].Data = append(tmp2[data[0]][data[1]].Data, expr)
		}
	}

	for key, items := range tmp2 {
		and := &Logical{Op: OpAnd}
		for _, or := range items {
			if len(or.Data) == 1 {
				and.Data = append(and.Data, or.Data[0])
			} else {
				and.Data = append(and.Data, or)
			}
		}
		switch len(and.Data) {
		case 0:
			continue
		case 1:
			result[key] = Filter{and.Data[0]}
		default:
			result[key] = Filter{and}
		}
	}

	return result
}

func fields(key string) []string {
	return strings.FieldsFunc(key, func(r rune) bool {
		return r == ']' || r == '['
	})
}

func value(values url.Values, fields []string) string {
	return values.Get(fmt.Sprintf("%s[%s][%s][%s]", slice.ToAny(fields)...))
}

func parse(val string, op Operator) any {
	if val == "" || op == OpRegex {
		return val
	}

	if strings.EqualFold(val, "null") || strings.EqualFold(val, "nil") {
		return nil
	}

	switch op {
	case OpIn, OpNin:
		data := strings.Split(val, ",")
		if newValue, ok := apply(data, cast.ToFloat64E); ok {
			return newValue
		}
		if newValue, ok := apply(data, cast.ToInt64E); ok {
			return newValue
		}
		if newValue, ok := apply(data, cast.ToTimeE); ok {
			return newValue
		}
		return data
	default:
		if newValue, ok := cast.ToFloat64E(val); ok == nil {
			return newValue
		}
		if newValue, ok := cast.ToInt64E(val); ok == nil {
			return newValue
		}
		if newValue, ok := cast.ToBoolE(val); ok == nil {
			return newValue
		}
		if newValue, ok := cast.ToTimeE(val); ok == nil {
			return newValue
		}
	}
	return val
}

func apply[T any](data []string, fn func(any) (T, error)) ([]any, bool) {
	if n, e := fn(data[0]); e == nil {
		newValue := []any{n}
		for k := 1; k < len(data); k++ {
			if n, e = fn(data[k]); e == nil {
				newValue = append(newValue, n)
			}
		}
		return newValue, true
	}
	return nil, false
}
