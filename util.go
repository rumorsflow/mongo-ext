package mongoext

import (
	"context"
	"fmt"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"time"
)

const (
	ErrMsgDecode    = "failed to decode document due to error: %w"
	ErrMsgQuery     = "failed to execute query due to error: %w"
	ErrMsgMarshal   = "failed to marshal document due to error: %w"
	ErrMsgUnmarshal = "failed to unmarshal document due to error: %w"
)

const (
	DefaultSize = int64(20)
	Timeout     = 5 * time.Second
)

func Save(ctx context.Context, c *mongo.Collection, filter any, update any, opts ...*options.UpdateOptions) (*mongo.UpdateResult, error) {
	ctx, cancel := context.WithTimeout(ctx, Timeout)
	defer cancel()

	opts = append(opts, options.Update().SetUpsert(true))

	result, err := c.UpdateOne(ctx, filter, update, opts...)
	if err != nil {
		return nil, fmt.Errorf(ErrMsgQuery, err)
	}
	return result, nil
}

func SaveMany(ctx context.Context, c *mongo.Collection, filter any, update any, opts ...*options.UpdateOptions) (*mongo.UpdateResult, error) {
	ctx, cancel := context.WithTimeout(ctx, Timeout)
	defer cancel()

	opts = append(opts, options.Update().SetUpsert(true))

	result, err := c.UpdateMany(ctx, filter, update, opts...)
	if err != nil {
		return nil, fmt.Errorf(ErrMsgQuery, err)
	}
	return result, nil
}

func BulkWrite(ctx context.Context, c *mongo.Collection, models []mongo.WriteModel, opts ...*options.BulkWriteOptions) (*mongo.BulkWriteResult, error) {
	ctx, cancel := context.WithTimeout(ctx, Timeout)
	defer cancel()

	result, err := c.BulkWrite(ctx, models, opts...)
	if err != nil {
		return nil, fmt.Errorf(ErrMsgQuery, err)
	}
	return result, nil
}

func Delete(ctx context.Context, c *mongo.Collection, filter any, opts ...*options.DeleteOptions) error {
	ctx, cancel := context.WithTimeout(ctx, Timeout)
	defer cancel()

	result, err := c.DeleteOne(ctx, filter, opts...)
	if err != nil {
		return fmt.Errorf(ErrMsgQuery, err)
	}
	if result.DeletedCount == 0 {
		return fmt.Errorf(ErrMsgQuery, mongo.ErrNoDocuments)
	}
	return nil
}

func DeleteMany(ctx context.Context, c *mongo.Collection, filter any, opts ...*options.DeleteOptions) error {
	ctx, cancel := context.WithTimeout(ctx, Timeout)
	defer cancel()

	result, err := c.DeleteMany(ctx, filter, opts...)
	if err != nil {
		return fmt.Errorf(ErrMsgQuery, err)
	}
	if result.DeletedCount == 0 {
		return fmt.Errorf(ErrMsgQuery, mongo.ErrNoDocuments)
	}
	return nil
}

func FindOne[T any](ctx context.Context, c *mongo.Collection, filter any, opts ...*options.FindOneOptions) (doc T, err error) {
	ctx, cancel := context.WithTimeout(ctx, Timeout)
	defer cancel()

	err = DecodeOne(c.FindOne(ctx, filter, opts...), &doc)
	return
}

func FindByCriteria[T any](ctx context.Context, c *mongo.Collection, criteria Criteria, opts ...*options.FindOptions) ([]T, error) {
	return Find[T](ctx, c, criteria.Filter, append(opts, Pagination(criteria.Index, criteria.Size).SetSort(criteria.Sort))...)
}

func Find[T any](ctx context.Context, c *mongo.Collection, filter any, opts ...*options.FindOptions) (docs []T, err error) {
	ctx, cancel := context.WithTimeout(ctx, Timeout)
	defer cancel()

	result, err := c.Find(ctx, filter, opts...)
	if err != nil {
		return nil, fmt.Errorf(ErrMsgQuery, err)
	}

	err = DecodeAll[T](ctx, result, &docs)
	return
}

func Count(ctx context.Context, c *mongo.Collection, filter any, opts ...*options.CountOptions) (int64, error) {
	ctx, cancel := context.WithTimeout(ctx, Timeout)
	defer cancel()

	count, err := c.CountDocuments(ctx, filter, opts...)
	if err != nil {
		return 0, fmt.Errorf(ErrMsgQuery, err)
	}
	return count, nil
}

func DecodeOne(r *mongo.SingleResult, doc any) error {
	if r.Err() != nil {
		return fmt.Errorf(ErrMsgQuery, r.Err())
	}
	if err := r.Decode(&doc); err != nil {
		return fmt.Errorf(ErrMsgDecode, err)
	}
	return nil
}

func DecodeAll(ctx context.Context, cur *mongo.Cursor, docs any) error {
	if cur.Err() != nil {
		return fmt.Errorf(ErrMsgQuery, cur.Err())
	}
	if err := cur.All(ctx, &docs); err != nil {
		return fmt.Errorf(ErrMsgDecode, err)
	}
	return nil
}

func ToBson(doc any) (bson.M, error) {
	data, err := bson.Marshal(doc)
	if err != nil {
		return nil, fmt.Errorf(ErrMsgMarshal, err)
	}

	var m bson.M
	if err = bson.Unmarshal(data, &m); err != nil {
		return nil, fmt.Errorf(ErrMsgUnmarshal, err)
	}
	return m, nil
}

func Pagination(index int64, size int64) *options.FindOptions {
	if index < 0 {
		index = 0
	}
	if size < 1 {
		size = DefaultSize
	}
	return options.Find().SetSkip(index).SetLimit(size)
}
