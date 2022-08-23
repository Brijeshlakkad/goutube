package pointcron

import "reflect"

func callJobFunc(jobFunc interface{}) {
	if jobFunc != nil {
		reflect.ValueOf(jobFunc).Call([]reflect.Value{})
	}
}
