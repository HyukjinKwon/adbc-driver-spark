// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements.  See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License.  You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package sparkconnect

import (
	"github.com/apache/arrow-adbc/go/adbc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// wrapGRPC converts a gRPC status error into an adbc.Error with a status code
// that maps cleanly onto the ADBC status space. This lets ADBC consumers reason
// about failures uniformly regardless of the backing driver.
func wrapGRPC(err error) error {
	if err == nil {
		return nil
	}
	st, ok := status.FromError(err)
	if !ok {
		return adbc.Error{Msg: err.Error(), Code: adbc.StatusInternal}
	}
	return adbc.Error{
		Msg:        st.Message(),
		Code:       grpcToADBC(st.Code()),
		VendorCode: int32(st.Code()),
	}
}

func grpcToADBC(code codes.Code) adbc.Status {
	switch code {
	case codes.OK:
		return adbc.StatusOK
	case codes.Canceled:
		return adbc.StatusCancelled
	case codes.InvalidArgument:
		return adbc.StatusInvalidArgument
	case codes.DeadlineExceeded:
		return adbc.StatusTimeout
	case codes.NotFound:
		return adbc.StatusNotFound
	case codes.AlreadyExists:
		return adbc.StatusAlreadyExists
	case codes.PermissionDenied, codes.Unauthenticated:
		return adbc.StatusUnauthorized
	case codes.Unimplemented:
		return adbc.StatusNotImplemented
	case codes.Unavailable:
		return adbc.StatusIO
	default:
		return adbc.StatusInternal
	}
}
