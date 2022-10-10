package mongoext

import (
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"strings"
)

const (
	// comparison
	OpEq  = Operator("$eq")
	OpNe  = Operator("$ne")
	OpGt  = Operator("$gt")
	OpGte = Operator("$gte")
	OpLt  = Operator("$lt")
	OpLte = Operator("$lte")
	OpIn  = Operator("$in")
	OpNin = Operator("$nin")

	// logical
	OpAnd = Operator("$and")
	OpOr  = Operator("$or")
	OpNor = Operator("$nor")
	OpNot = Operator("$not")

	// array
	OpAll       = Operator("$all")
	OpSize      = Operator("$size")
	OpElemMatch = Operator("$elemMatch")

	// element
	OpExists = Operator("$exists")

	// evaluation
	OpRegex = Operator("$regex")
)

type (
	Filter []Expr

	Expr interface {
		Build() any
	}

	Operator string

	Logical struct {
		Op   Operator
		Data []Expr
	}

	Field struct {
		Op    Operator
		Name  string
		Value any
	}
)

func And(expr ...Expr) Expr {
	return &Logical{Op: OpAnd, Data: expr}
}

func Or(expr ...Expr) Expr {
	return &Logical{Op: OpOr, Data: expr}
}

func Nor(expr ...Expr) Expr {
	return &Logical{Op: OpNor, Data: expr}
}

func Not(name string, expr Expr) Expr {
	return &Field{Name: name, Op: OpNot, Value: expr}
}

func Eq(name string, value any) Expr {
	return &Field{Name: name, Op: OpEq, Value: value}
}

func Ne(name string, value any) Expr {
	return &Field{Name: name, Op: OpNe, Value: value}
}

func Gt(name string, value any) Expr {
	return &Field{Name: name, Op: OpGt, Value: value}
}

func Gte(name string, value any) Expr {
	return &Field{Name: name, Op: OpGte, Value: value}
}

func Lt(name string, value any) Expr {
	return &Field{Name: name, Op: OpLt, Value: value}
}

func Lte(name string, value any) Expr {
	return &Field{Name: name, Op: OpLte, Value: value}
}

func In(name string, value ...any) Expr {
	return &Field{Name: name, Op: OpIn, Value: value}
}

func Nin(name string, value ...any) Expr {
	return &Field{Name: name, Op: OpNin, Value: value}
}

func Size(name string, value uint) Expr {
	return &Field{Name: name, Op: OpSize, Value: value}
}

func Exists(name string, value bool) Expr {
	return &Field{Name: name, Op: OpExists, Value: value}
}

func Regex(name string, pattern string, opts ...string) Expr {
	return &Field{Name: name, Op: OpRegex, Value: primitive.Regex{
		Pattern: pattern,
		Options: strings.Join(opts, ""),
	}}
}

func All(name string, value ...any) Expr {
	return &Field{Name: name, Op: OpAll, Value: value}
}

func ElemMatch(name string, query ...Expr) Expr {
	value := bson.M{}
	for _, e := range query {
		if m, ok := e.Build().(bson.M); ok {
			if f, ok := e.(*Field); ok && len(f.Name) > 0 {
				value[f.Name] = m
			} else {
				for k, v := range m {
					value[k] = v
				}
			}
		}

	}
	return &Field{Name: name, Op: OpElemMatch, Value: value}
}

func (l *Logical) Build() any {
	result := make(bson.A, len(l.Data))
	for i, item := range l.Data {
		switch e := item.(type) {
		case *Logical:
			result[i] = bson.M{string(e.Op): e.Build()}
			continue
		case *Field:
			if len(e.Name) > 0 {
				result[i] = bson.M{e.Name: e.Build()}
				continue
			}
		}
		result[i] = item.Build()
	}
	return result
}

func (f *Field) Build() any {
	return bson.M{string(f.Op): f.Value}
}

func (f Filter) Build() any {
	if len(f) == 0 {
		return bson.D{}
	}

	result := bson.M{}
	for _, i := range f {
		switch e := i.(type) {
		case *Logical:
			result[string(e.Op)] = e.Build()
		case *Field:
			if len(e.Name) > 0 {
				result[e.Name] = e.Build()
			} else {
				if m, ok := e.Build().(bson.M); ok {
					for k, v := range m {
						result[k] = v
					}
				}
			}
		}
	}
	return result
}
