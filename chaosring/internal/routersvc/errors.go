package routersvc

import (
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func errPermission(format string, a ...any) error {
	return status.Errorf(codes.PermissionDenied, format, a...)
}

func errNotFound(format string, a ...any) error {
	return status.Errorf(codes.NotFound, format, a...)
}

func errFailedPrecondition(format string, a ...any) error {
	return status.Errorf(codes.FailedPrecondition, format, a...)
}

func errInvalidArg(format string, a ...any) error {
	return status.Errorf(codes.InvalidArgument, format, a...)
}

func errInternal(format string, a ...any) error {
	return status.Errorf(codes.Internal, format, a...)
}

func errUnavailable(format string, a ...any) error {
	return status.Errorf(codes.Unavailable, format, a...)
}

func errAborted(format string, a ...any) error {
	return status.Errorf(codes.Aborted, format, a...)
}

func errRetired(format string, a ...any) error {
	// 用自定义语义: 旧环退役。复用 FailedPrecondition, 由消息区分。
	return status.Errorf(codes.FailedPrecondition, "RETIRED: "+format, a...)
}
