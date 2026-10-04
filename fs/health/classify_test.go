package health

import (
	"context"
	"errors"
	"fmt"
	"io"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/fserrors"
)

func TestClassify(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
		want Kind
	}{
		{"canceled", context.Canceled, kindIgnore},
		{"canceled wrapped", fmt.Errorf("upload stopped: %w", context.Canceled), kindIgnore},
		{"deadline exceeded", context.DeadlineExceeded, kindRetriable},
		{"no retry", fserrors.NoRetryError(errors.New("bad request")), kindDeterministic},
		{"no low level retry", fserrors.NoLowLevelRetryError(errors.New("bad request")), kindDeterministic},
		{"retry marked", fserrors.RetryError(io.EOF), kindRetriable},
		{"retry after", fserrors.NewErrorRetryAfter(0), kindRetriable},
		{"retriable network", io.EOF, kindRetriable},
		{"object not found", fs.ErrorObjectNotFound, kindDeterministic},
		{"object not found wrapped", fmt.Errorf("stat failed: %w", fs.ErrorObjectNotFound), kindDeterministic},
		{"permission denied", fs.ErrorPermissionDenied, kindDeterministic},
		{"directory not found", fs.ErrorDirNotFound, kindDeterministic},
		{"is dir", fs.ErrorIsDir, kindDeterministic},
		{"http 400", errors.New(`HTTP error 400 (400 Bad Request) returned body: ""`), kindDeterministic},
		{"http 401", errors.New(`HTTP error 401 (401 Unauthorized) returned body: ""`), kindDeterministic},
		{"http 403", errors.New(`HTTP error 403 (403 Forbidden) returned body: ""`), kindDeterministic},
		{"http 404", errors.New(`HTTP error 404 (404 Not Found) returned body: ""`), kindDeterministic},
		{"http 410", errors.New(`HTTP error 410 (410 Gone) returned body: ""`), kindDeterministic},
		{"http 408", errors.New("http error 408: 408 Request Timeout"), kindRetriable},
		{"http 429", errors.New(`HTTP error 429 (429 Too Many Requests) returned body: ""`), kindRetriable},
		{"http 500", errors.New(`HTTP error 500 (500 Internal Server Error) returned body: ""`), kindRetriable},
		{"http 503", errors.New(`HTTP error 503 (503 Service Unavailable) returned body: ""`), kindRetriable},
		{"bare 401", errors.New("couldn't list files: 401 Unauthorized"), kindDeterministic},
		{"bare 404", errors.New("stat failed: 404 Not Found"), kindDeterministic},
		{"bare 403", errors.New("403 Forbidden"), kindDeterministic},
		{"bare 429 parenthesised", errors.New("read metadata failed: (429 Too Many Requests)"), kindRetriable},
		{"bare 503 wrapped", errors.New("upload failed: 503 Service Unavailable: try later"), kindRetriable},
		{"unrelated number not a status", errors.New("processed 401 records in 550 files"), kindRetriable},
		{"aws v2 403", errors.New("https response error StatusCode: 403, RequestID: ABC, HostID: xyz"), kindDeterministic},
		{"aws v2 401", errors.New("https response error StatusCode: 401, RequestID: ABC"), kindDeterministic},
		{"aws v2 500", errors.New("https response error StatusCode: 500, RequestID: ABC"), kindRetriable},
		{"aws v1 403", errors.New("Forbidden: Forbidden status code: 403, request id: ABC"), kindDeterministic},
		{"aws v1 404", errors.New("NotFound: status code: 404, request id: ABC"), kindDeterministic},
		{"aws v1 503", errors.New("ServiceUnavailable: status code: 503, request id: ABC"), kindRetriable},
		{"googleapi 403", errors.New("googleapi: Error 403: insufficientPermissions: The user does not have sufficient permissions"), kindDeterministic},
		{"googleapi 401", errors.New("googleapi: Error 401: invalid_grant"), kindDeterministic},
		{"googleapi 404", errors.New("googleapi: Error 404: notFound: File not found"), kindDeterministic},
		{"googleapi 429", errors.New("googleapi: Error 429: userRateLimitExceeded"), kindRetriable},
		{"googleapi 500", errors.New("googleapi: Error 500: internalError"), kindRetriable},
		{"dir exists sentinel", fs.ErrorDirExists, kindDeterministic},
		{"unknown defaults retriable", errors.New("something odd happened"), kindRetriable},
	} {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, Classify(tc.err))
		})
	}
}
