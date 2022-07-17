package pointcron

import (
	"reflect"
	"time"
)

type PointJob interface {
	Close() error
	GetLastAccessed() time.Time
}

func callJobFunc(jobFunc interface{}) {
	if jobFunc != nil {
		reflect.ValueOf(jobFunc).Call([]reflect.Value{})
	}
}

func callGetLastAccessed(jobFunc interface{}) time.Time {
	if jobFunc != nil {
		result := reflect.ValueOf(jobFunc).Call([]reflect.Value{})
		return result[0].Interface().(time.Time)
	}
	return time.Time{}
}
