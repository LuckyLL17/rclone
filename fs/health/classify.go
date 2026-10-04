package health

import (
	"context"
	"errors"
	"net/http"
	"regexp"
	"strconv"
	"strings"

	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/fserrors"
)

// Kind classifies the effect an error has on the circuit breaker.
type Kind int

const (
	// kindIgnore marks errors which carry no health signal, e.g. a
	// caller cancelling its own request.
	kindIgnore Kind = iota
	// kindRetriable marks transient transport, rate limiting and
	// server errors that justify tripping the breaker when repeated.
	kindRetriable
	// kindDeterministic marks request, authentication, permission and
	// path errors that retrying against the same remote cannot fix.
	kindDeterministic
	// kindSuccess is the outcome of a nil error.
	kindSuccess
)

// deterministicSentinels are rclone errors which state that the
// request itself (path, permission, object state) is the problem, so
// they must never trip the breaker.
var deterministicSentinels = []error{
	fs.ErrorObjectNotFound,
	fs.ErrorDirNotFound,
	fs.ErrorPermissionDenied,
	fs.ErrorIsDir,
	fs.ErrorIsFile,
	fs.ErrorNotAFile,
	fs.ErrorFileNameTooLong,
	fs.ErrorListBucketRequired,
	fs.ErrorCantListRoot,
	fs.ErrorDirectoryNotEmpty,
	fs.ErrorImmutableModified,
	fs.ErrorDirExists,
}

// HTTP status codes embedded in backend error strings take several
// shapes depending on the HTTP stack:
//   - lib/rest:          `HTTP error 403 (403 Forbidden) ...`
//   - aws-sdk-go v2:     `https response error StatusCode: 403, RequestID: ...`
//   - aws-sdk-go v1:     `Forbidden: Forbidden status code: 403, request id: ...`
//   - google api client: `googleapi: Error 403: ...`
//   - bare status:       `401 Unauthorized`
var httpStatusRegexps = []*regexp.Regexp{
	regexp.MustCompile(`(?i)http error (\d{3})`),
	regexp.MustCompile(`(?i)status\s*code\s*:\s*(\d{3})`),
	regexp.MustCompile(`googleapi:\s*[Ee]rror\s+(\d{3})`),
}

// bareStatusRegexp matches a status code immediately followed by its
// standard status phrase, e.g. "401 Unauthorized" as produced by
// backends formatting responses without lib/rest.
var bareStatusRegexp = regexp.MustCompile(`(?:^|[\s(:])(\d{3})[\s\t]+([A-Z][A-Za-z ]+)`)

// httpStatus returns an HTTP response status code present in err's
// message, if any.
func httpStatus(err error) (int, bool) {
	for _, rx := range httpStatusRegexps {
		if match := rx.FindStringSubmatch(err.Error()); match != nil {
			if code, convErr := strconv.Atoi(match[1]); convErr == nil {
				return code, true
			}
		}
	}
	for _, match := range bareStatusRegexp.FindAllStringSubmatch(err.Error(), -1) {
		code, convErr := strconv.Atoi(match[1])
		if convErr != nil || code < 400 || code >= 600 {
			continue
		}
		// Require the standard phrase to avoid reading unrelated
		// three digit numbers as response codes.
		phrase := http.StatusText(code)
		if phrase != "" && strings.HasPrefix(strings.ToLower(strings.TrimSpace(match[2])), strings.ToLower(phrase)) {
			return code, true
		}
	}
	return 0, false
}

// Classify decides how err affects the circuit breaker.
//
// nil is treated as success by callers and never reaches Classify.
func Classify(err error) Kind {
	if errors.Is(err, context.Canceled) {
		return kindIgnore
	}
	// Explicit backend decisions take precedence over everything else.
	if fserrors.IsNoRetryError(err) || fserrors.IsNoLowLevelRetryError(err) {
		return kindDeterministic
	}
	if fserrors.IsRetryError(err) ||
		fserrors.IsRetryAfterError(err) ||
		fserrors.ShouldRetry(err) ||
		errors.Is(err, context.DeadlineExceeded) {
		return kindRetriable
	}
	for _, sentinel := range deterministicSentinels {
		if errors.Is(err, sentinel) {
			return kindDeterministic
		}
	}
	if code, ok := httpStatus(err); ok {
		switch {
		case code == 408 || code == 429 || code >= 500:
			return kindRetriable
		case code >= 400 && code < 500:
			return kindDeterministic
		}
	}
	// An unclassified failure is assumed transient: a success resets
	// the consecutive count, so novel errors trip the breaker only when
	// they persist.
	return kindRetriable
}
