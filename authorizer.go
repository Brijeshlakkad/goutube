package goutube

import (
	"fmt"

	"github.com/casbin/casbin"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type authorizer struct {
	enforcer *casbin.Enforcer
}

func (a *authorizer) Authorize(subject, object, action string) error {
	if !a.enforcer.Enforce(subject, object, action) {
		msg := fmt.Sprintf(
			"%s not to %s to %s",
			subject,
			action,
			object,
		)
		st := status.New(codes.PermissionDenied, msg)
		return st.Err()
	}
	return nil
}

func newAuth(model, policy string) *authorizer {
	enforcer := casbin.NewEnforcer(model, policy)
	return &authorizer{
		enforcer: enforcer,
	}
}
