package streaming_v1

import (
	"fmt"

	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/status"
)

type PointNotFound struct {
	PointId string
}

func (e PointNotFound) GRPCStatus() *status.Status {
	st := status.New(
		404,
		fmt.Sprintf("point is not found: %s", e.PointId),
	)
	msg := fmt.Sprintf(
		"The requested point is not found: %s",
		e.PointId,
	)

	d := &errdetails.LocalizedMessage{
		Locale:  "en-US",
		Message: msg,
	}
	std, err := st.WithDetails(d)
	if err != nil {
		return st
	}
	return std
}

func (e PointNotFound) Error() string {
	return e.GRPCStatus().Err().Error()
}

type LocusNotFound struct {
	LocusId string
}

func (e LocusNotFound) GRPCStatus() *status.Status {
	st := status.New(
		404,
		fmt.Sprintf("locus is not found: %s", e.LocusId),
	)
	msg := fmt.Sprintf(
		"The requested locus is not found: %s",
		e.LocusId,
	)

	d := &errdetails.LocalizedMessage{
		Locale:  "en-US",
		Message: msg,
	}
	std, err := st.WithDetails(d)
	if err != nil {
		return st
	}
	return std
}

func (e LocusNotFound) Error() string {
	return e.GRPCStatus().Err().Error()
}
