package fs

import "context"

// ListRResume carries continuation state for a single paged ListR
// call through the context.
//
// Callers set Token to the continuation token of a previous page
// ("" for the first page) and PageSize to the desired number of
// entries per page (0 for the backend default). The backend writes
// Token and More back before returning: Token holds the
// continuation token for the next page and More reports whether
// further pages remain.
type ListRResume struct {
	Token    string // in: continuation token, out: next page token
	More     bool   // out: whether more pages remain
	PageSize int    // in: requested entries per page, 0 for default
}

type listRResumeContextKeyType struct{}

var listRResumeContextKey = listRResumeContextKeyType{}

// WithListRResume returns a context carrying the ListR continuation
// state passed in.
func WithListRResume(ctx context.Context, resume *ListRResume) context.Context {
	return context.WithValue(ctx, listRResumeContextKey, resume)
}

// GetListRResume returns the ListR continuation state carried in the
// context, or nil if none is present.
func GetListRResume(ctx context.Context) *ListRResume {
	resume, _ := ctx.Value(listRResumeContextKey).(*ListRResume)
	return resume
}
