// Package capability provides the capability command.
package capability

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/rclone/rclone/cmd"
	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/capcache"
	"github.com/rclone/rclone/fs/config/flags"
	"github.com/spf13/cobra"
)

var useJSON bool

func init() {
	cmd.Root.AddCommand(commandDefinition)
	commandDefinition.AddCommand(showCommand, listCommand, refreshCommand, invalidateCommand, clearCommand)
	cmdFlags := commandDefinition.PersistentFlags()
	flags.BoolVarP(cmdFlags, &useJSON, "json", "", useJSON, "Output in JSON format", "")
}

var commandDefinition = &cobra.Command{
	Use:   "capability <command> remote:path [flags]",
	Short: `Inspect and manage cached remote capability profiles.`,
	Long: `This manages the optional cache of remote capability profiles:
feature flags, supported hashes, mime types, server-side copy and
listing capabilities.

Enable the cache with --capability-cache; profiles are then keyed by
backend type, a fingerprint of the resolved configuration and the
authenticated user context, and shared by copy, sync, check and list
operations. Use the subcommands to show, list, refresh or invalidate
profiles.
`,
	Annotations: map[string]string{
		"versionIntroduced": "v1.77",
		"groups":            "Config",
	},
}

var showCommand = &cobra.Command{
	Use:   "show remote:path",
	Short: `Show the capability profile of a remote.`,
	Long: `Shows the capability profile for the remote. A fresh profile is
returned without probing if cached, otherwise the remote is probed.
`,
	Run: func(command *cobra.Command, args []string) {
		cmd.CheckArgs(1, 1, command, args)
		fsrc := cmd.NewFsSrc(args)
		cmd.Run(false, false, command, func() error {
			p, err := capcache.Get(context.Background(), fsrc)
			if err != nil {
				return err
			}
			return writeProfile(p)
		})
	},
}

var listCommand = &cobra.Command{
	Use:   "list",
	Short: `List all cached capability profiles.`,
	Run: func(command *cobra.Command, args []string) {
		cmd.CheckArgs(0, 0, command, args)
		cmd.Run(false, true, command, func() error {
			return writeProfiles(capcache.List())
		})
	},
}

var refreshCommand = &cobra.Command{
	Use:   "refresh remote:path",
	Short: `Re-probe the remote and refresh its capability profile.`,
	Run: func(command *cobra.Command, args []string) {
		cmd.CheckArgs(1, 1, command, args)
		fsrc := cmd.NewFsSrc(args)
		cmd.Run(false, false, command, func() error {
			p, err := capcache.Refresh(context.Background(), fsrc)
			if err != nil {
				return err
			}
			return writeProfile(p)
		})
	},
}

var invalidateCommand = &cobra.Command{
	Use:   "invalidate remote:path",
	Short: `Remove the capability profile of the remote.`,
	Run: func(command *cobra.Command, args []string) {
		cmd.CheckArgs(1, 1, command, args)
		fsrc := cmd.NewFsSrc(args)
		cmd.Run(false, false, command, func() error {
			deleted, err := capcache.Invalidate(context.Background(), fsrc)
			if err != nil {
				return err
			}
			if deleted {
				fmt.Printf("Invalidated capability profile for %s\n", fsrc.String())
			} else {
				fmt.Printf("No cached capability profile for %s\n", fsrc.String())
			}
			return nil
		})
	},
}

var clearBackend string

var clearCommand = &cobra.Command{
	Use:   "clear",
	Short: `Clear capability profiles, optionally for one backend type.`,
	Run: func(command *cobra.Command, args []string) {
		cmd.CheckArgs(0, 0, command, args)
		cmd.Run(false, true, command, func() error {
			if clearBackend != "" {
				deleted, err := capcache.InvalidateBackend(clearBackend)
				if err != nil {
					return err
				}
				fmt.Printf("Removed %d capability profiles for backend %s\n", deleted, clearBackend)
			} else {
				if err := capcache.Clear(); err != nil {
					return err
				}
				fmt.Println("Cleared all capability profiles")
			}
			return nil
		})
	},
}

func init() {
	cmdFlags := clearCommand.Flags()
	flags.StringVarP(cmdFlags, &clearBackend, "backend", "", clearBackend, "Clear profiles for the backend type only", "")
}

// featureGroups renders named subsets of the feature flags.
var featureGroups = []struct {
	name  string
	flags []string
}{
	{"MimeType", []string{"ReadMimeType", "WriteMimeType"}},
	{"ServerSide", []string{"Copy", "Move", "DirMove", "ServerSideAcrossConfigs"}},
	{"Listing", []string{"ListR", "ListP", "FilterAware"}},
}

func groupedFlags() map[string]bool {
	out := map[string]bool{}
	for _, group := range featureGroups {
		for _, name := range group.flags {
			out[name] = true
		}
	}
	return out
}

func enabledNames(features map[string]bool, names []string) []string {
	var out []string
	for _, name := range names {
		if features[name] {
			out = append(out, name)
		}
	}
	if out == nil {
		return []string{"-"}
	}
	return out
}

// writeProfile writes one profile as JSON or grouped text.
func writeProfile(p *capcache.Profile) error {
	if useJSON {
		data, err := json.MarshalIndent(p, "", "\t")
		if err != nil {
			return err
		}
		fmt.Println(string(data))
		return nil
	}
	fmt.Printf("Backend: %s\n", p.Key.Backend)
	fmt.Printf("Name: %s\n", p.Name)
	fmt.Printf("Root: %s\n", p.Root)
	fmt.Printf("Precision: %s\n", fs.Duration(p.Precision).String())
	fmt.Printf("Fetched: %s\n", p.FetchedAt.Format(time.RFC3339))
	if p.ExpiresAt.IsZero() {
		fmt.Println("Expires: never")
	} else {
		state := ""
		if p.Expired() {
			state = " (stale)"
		}
		fmt.Printf("Expires: %s%s\n", p.ExpiresAt.Format(time.RFC3339), state)
	}
	if len(p.Hashes) == 0 {
		fmt.Println("Hashes: none")
	} else {
		fmt.Printf("Hashes: %s\n", strings.Join(p.Hashes, ", "))
	}
	for _, group := range featureGroups {
		fmt.Printf("%s: %s\n", group.name, strings.Join(enabledNames(p.Features, group.flags), ", "))
	}
	known := groupedFlags()
	var others []string
	for name, enabled := range p.Features {
		if enabled && !known[name] {
			others = append(others, name)
		}
	}
	sort.Strings(others)
	if len(others) == 0 {
		fmt.Println("Features: -")
	} else {
		fmt.Printf("Features: %s\n", strings.Join(others, ", "))
	}
	return nil
}

// writeProfiles writes all profiles as JSON or one summary line each.
func writeProfiles(profiles []*capcache.Profile) error {
	if useJSON {
		data, err := json.MarshalIndent(profiles, "", "\t")
		if err != nil {
			return err
		}
		fmt.Println(string(data))
		return nil
	}
	if len(profiles) == 0 {
		fmt.Println("No capability profiles cached")
		return nil
	}
	for _, p := range profiles {
		expires := "never"
		if !p.ExpiresAt.IsZero() {
			expires = fs.Duration(time.Until(p.ExpiresAt)).ShortReadableString()
			if p.Expired() {
				expires = "stale"
			}
		}
		fmt.Printf("%s\t%s:%s\texpires=%s\tcopy=%v\tlistR=%v\thashes=%d\n",
			p.Key.Backend, p.Name, p.Root, expires, p.Features["Copy"], p.Features["ListR"], len(p.Hashes))
	}
	return nil
}
