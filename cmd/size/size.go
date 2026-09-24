// Package size provides the size command.
package size

import (
	"context"
	"encoding/json"
	"os"
	"strconv"
	"time"

	"github.com/rclone/rclone/cmd"
	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/config/flags"
	"github.com/rclone/rclone/fs/listcursor"
	"github.com/rclone/rclone/fs/operations"
	"github.com/rclone/rclone/fs/walk"
	"github.com/spf13/cobra"
)

var jsonOutput bool

func init() {
	cmd.Root.AddCommand(commandDefinition)
	cmdFlags := commandDefinition.Flags()
	flags.BoolVarP(cmdFlags, &jsonOutput, "json", "", false, "Format output as JSON", "")
}

var commandDefinition = &cobra.Command{
	Use:   "size remote:path",
	Short: `Prints the total size and number of objects in remote:path.`,
	Long: `Counts objects in the path and calculates the total size. Prints the
result to standard output.

By default the output is in human-readable format, but shows values in
both human-readable format as well as the raw numbers (global option
` + "`--human-readable`" + ` is not considered). Use option ` + "`--json`" + `
to format output as JSON instead.

Recurses by default, use ` + "`--max-depth 1`" + ` to stop the
recursion.

The listing can be resumed with the global ` + "`--list-cursor-in`" + `
option. In ` + "`--json`" + ` mode a single page is counted and the
reply carries the next cursor until the listing is done; in the normal
output mode remaining pages are fetched automatically.

Some backends do not always provide file sizes, see for example
[Google Photos](/googlephotos/#size) and
[Google Docs](/drive/#limitations-of-google-docs).
Rclone will then show a notice in the log indicating how many such
files were encountered, and count them in as empty files in the output
of the size command.`,
	Annotations: map[string]string{
		"versionIntroduced": "v1.23",
		"groups":            "Filter,Listing",
	},
	Run: func(command *cobra.Command, args []string) {
		cmd.CheckArgs(1, 1, command, args)
		fsrc := cmd.NewFsSrc(args)
		cmd.Run(false, false, command, func() error {
			ctx := context.Background()
			ci := fs.GetConfig(ctx)
			var results struct {
				Count    int64  `json:"count"`
				Bytes    int64  `json:"bytes"`
				Sizeless int64  `json:"sizeless"`
				Fs       string `json:"fs"`
				Cursor   string `json:"cursor,omitempty"`
				Done     bool   `json:"done,omitempty"`
			}

			addPage := func(entries fs.DirEntries) {
				for _, entry := range entries {
					o, ok := entry.(fs.Object)
					if !ok {
						continue
					}
					results.Count++
					objectSize := o.Size()
					if objectSize < 0 {
						results.Sizeless++
					} else if objectSize > 0 {
						results.Bytes += objectSize
					}
				}
			}

			var err error
			paged := ci.ListCursorIn != "" || (jsonOutput && ci.ListPageSize > 0)
			if paged {
				cur, err := listcursor.DecodeCursor(ci.ListCursorIn)
				if err != nil {
					return err
				}
				pageOpt := listcursor.Options{
					MaxLevel: ci.MaxDepth,
					ListType: walk.ListObjects,
					PageSize: ci.ListPageSize,
					TTL:      time.Duration(ci.ListCursorTTL),
				}
				if jsonOutput {
					// A single page per invocation so a paged caller
					// can accumulate the counts itself.
					var page fs.DirEntries
					var next *listcursor.Cursor
					page, next, err = listcursor.ListPage(ctx, fsrc, "", pageOpt, cur)
					if err != nil {
						return err
					}
					addPage(page)
					results.Done = next == nil
					if next != nil {
						encoded, encodeErr := next.Encode()
						if encodeErr != nil {
							return encodeErr
						}
						results.Cursor = encoded
					}
				} else {
					// Transparently page through the remainder.
					for {
						var page fs.DirEntries
						var next *listcursor.Cursor
						page, next, err = listcursor.ListPage(ctx, fsrc, "", pageOpt, cur)
						if err != nil {
							return err
						}
						addPage(page)
						if next == nil {
							break
						}
						cur = next
					}
				}
			} else {
				results.Count, results.Bytes, results.Sizeless, err = operations.Count(ctx, fsrc)
				if err != nil {
					return err
				}
			}
			if results.Sizeless > 0 {
				fs.Logf(fsrc, "Size may be underestimated due to %d objects with unknown size", results.Sizeless)
			}
			if jsonOutput {
				results.Fs = args[0]
				return json.NewEncoder(os.Stdout).Encode(results)
			}
			count := strconv.FormatInt(results.Count, 10)
			countSuffix := fs.CountSuffix(results.Count).String()
			if count == countSuffix {
				operations.SyncPrintf("Total objects: %s\n", count)
			} else {
				operations.SyncPrintf("Total objects: %s (%s)\n", countSuffix, count)
			}
			operations.SyncPrintf("Total size: %s (%d Byte)\n", fs.SizeSuffix(results.Bytes).ByteUnit(), results.Bytes)
			if results.Sizeless > 0 {
				operations.SyncPrintf("Total objects with unknown size: %s (%d)\n", fs.CountSuffix(results.Sizeless), results.Sizeless)
			}
			return nil
		})
	},
}
