// RC interface for the remote capability profile cache

package rc

import (
	"context"

	"github.com/rclone/rclone/fs/capcache"
)

func init() {
	Add(Call{
		Path:  "capcache/list",
		Fn:    rcCapCacheList,
		Title: "List all cached remote capability profiles.",
		Help: `This returns the capability profiles stored in the cache, sorted by key.

Returns:
- profiles - array of capability profiles
`,
	})
}

// List the capability cache
func rcCapCacheList(ctx context.Context, in Params) (out Params, err error) {
	return Params{
		"profiles": capcache.List(),
	}, nil
}

func init() {
	Add(Call{
		Path:  "capcache/get",
		Fn:    rcCapCacheGet,
		Title: "Get the capability profile for the remote.",
		Help: `This takes the following parameters:

- fs - a remote name string e.g. "drive:"

If a fresh profile is cached it is returned without probing, otherwise
the remote is probed and the profile is stored when the capability
cache is enabled.

Returns:
- profile - capability profile
`,
	})
}

// Get a capability profile
func rcCapCacheGet(ctx context.Context, in Params) (out Params, err error) {
	f, err := GetFs(ctx, in)
	if err != nil {
		return nil, err
	}
	profile, err := capcache.Get(ctx, f)
	if err != nil {
		return nil, err
	}
	return Params{"profile": profile}, nil
}

func init() {
	Add(Call{
		Path:  "capcache/refresh",
		Fn:    rcCapCacheRefresh,
		Title: "Refresh the capability profile for the remote.",
		Help: `This takes the following parameters:

- fs - a remote name string e.g. "drive:"

This re-probes the remote immediately, ignoring the TTL, and stores
the new capability profile.

Returns:
- profile - capability profile
`,
	})
}

// Refresh a capability profile
func rcCapCacheRefresh(ctx context.Context, in Params) (out Params, err error) {
	f, err := GetFs(ctx, in)
	if err != nil {
		return nil, err
	}
	profile, err := capcache.Refresh(ctx, f)
	if err != nil {
		return nil, err
	}
	return Params{"profile": profile}, nil
}

func init() {
	Add(Call{
		Path:  "capcache/invalidate",
		Fn:    rcCapCacheInvalidate,
		Title: "Invalidate the capability profile for the remote.",
		Help: `This takes the following parameters:

- fs - a remote name string e.g. "drive:"

This removes the stored capability profile, forcing a fresh probe on
the next call regardless of TTL.

Returns:
- deleted - true if an entry was removed
`,
	})
}

// Invalidate a capability profile
func rcCapCacheInvalidate(ctx context.Context, in Params) (out Params, err error) {
	f, err := GetFs(ctx, in)
	if err != nil {
		return nil, err
	}
	deleted, err := capcache.Invalidate(ctx, f)
	if err != nil {
		return nil, err
	}
	return Params{"deleted": deleted}, nil
}

func init() {
	Add(Call{
		Path:  "capcache/clear",
		Fn:    rcCapCacheClear,
		Title: "Clear capability profiles, optionally for one backend type.",
		Help: `This takes the following parameters:

- backend - backend type e.g. "s3" (optional)

Without the backend parameter all profiles are removed; with it only
profiles for that backend type are removed.

Returns:
- deleted - number of entries removed
`,
	})
}

// Clear the capability cache
func rcCapCacheClear(ctx context.Context, in Params) (out Params, err error) {
	backend, err := in.GetString("backend")
	if IsErrParamNotFound(err) {
		if err := capcache.Clear(); err != nil {
			return nil, err
		}
		return Params{"deleted": "all"}, nil
	}
	if err != nil {
		return nil, err
	}
	deleted, err := capcache.InvalidateBackend(backend)
	if err != nil {
		return nil, err
	}
	return Params{"deleted": deleted}, nil
}
