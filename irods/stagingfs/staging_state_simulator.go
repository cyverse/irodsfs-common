package stagingfs

import (
	"bufio"
	"fmt"
	"os"
	"strings"
)

// RunSimulator starts the staging state simulator for testing
func RunSimulator() {
	sm := NewStagingStateManager()

	fmt.Println("🚀 Staging State Simulator")
	fmt.Println("Commands: create <path>, modify <path>, rename <old> <new>, delete <path>, sync, status, exit")

	scanner := bufio.NewScanner(os.Stdin)

	for {
		fmt.Print("> ")
		if !scanner.Scan() {
			break
		}

		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}

		parts := strings.Fields(line)
		cmd := parts[0]

		switch cmd {
		case "create":
			if len(parts) < 2 {
				fmt.Println("❌ Usage: create <path>")
				continue
			}
			if err := sm.Create(parts[1]); err != nil {
				fmt.Printf("❌ Error: %v\n\n", err)
				continue
			}
			meta := sm.Get(parts[1])
			printMetadata(parts[1], "CREATE", meta)

		case "modify":
			if len(parts) < 2 {
				fmt.Println("❌ Usage: modify <path>")
				continue
			}
			if err := sm.Modify(parts[1]); err != nil {
				fmt.Printf("❌ Error: %v\n\n", err)
				continue
			}
			meta := sm.Get(parts[1])
			printMetadata(parts[1], "MODIFY", meta)

		case "rename":
			if len(parts) < 3 {
				fmt.Println("❌ Usage: rename <old> <new>")
				continue
			}
			syncNow, err := sm.Rename(parts[1], parts[2])
			if err != nil {
				fmt.Printf("❌ Error: %v\n\n", err)
				continue
			}

			if syncNow {
				fmt.Printf("⚡ SYNC_RENAME: %s → %s (immediate iRODS sync)\n", parts[1], parts[2])
				if meta := sm.Get(parts[1]); meta != nil {
					fmt.Printf("⚡ Preceding action: PUT (%s)\n", parts[1])
				}
				fmt.Printf("└─ iRODS operation: RENAME (%s → %s)\n\n", parts[1], parts[2])
			} else {
				meta := sm.Get(parts[2])
				printMetadata(parts[2], "RENAME", meta)
			}

		case "delete":
			if len(parts) < 2 {
				fmt.Println("❌ Usage: delete <path>")
				continue
			}
			if err := sm.Delete(parts[1]); err != nil {
				fmt.Printf("❌ Error: %v\n\n", err)
				continue
			}
			if meta := sm.Get(parts[1]); meta == nil {
				fmt.Printf("❌ DELETE %s → metadata removed\n\n", parts[1])
			} else {
				printMetadata(parts[1], "DELETE", meta)
			}

		case "sync":
			all := sm.GetAll()
			if len(all) == 0 {
				fmt.Println("📭 No items to sync")
				continue
			}

			fmt.Println("🔄 Grace Period expired - sync starting")
			for path, meta := range all {
				switch meta.Action {
				case ActionUpload:
					fmt.Printf("✓ PUT: %s\n", path)
				case ActionDelete:
					fmt.Printf("✓ DELETE: %s\n", path)
				case ActionRename:
					fmt.Printf("✓ RENAME: %s\n", path)
				}
			}

			if err := sm.SyncAll(); err != nil {
				fmt.Printf("❌ Sync error: %v\n\n", err)
				continue
			}
			fmt.Println("\n✅ Sync completed - metadata cleared")

		case "status":
			printSimulatorStatus(sm)

		case "exit":
			fmt.Println("👋 Exiting")
			return

		default:
			fmt.Println("❌ Unknown command")
		}
	}
}

func printMetadata(path string, op string, meta *StagingMetadata) {
	if meta == nil {
		fmt.Printf("❌ %s %s → metadata removed\n", op, path)
		return
	}

	fmt.Printf("┌─ %s %s\n", op, path)
	fmt.Printf("├─ Path: %s\n", meta.Path)
	fmt.Printf("├─ Action: %s\n", meta.Action)
	fmt.Printf("├─ IsNew: %v\n", meta.IsNew)
	fmt.Printf("└─ iRODS operation: %s\n\n", getIRODSOperation(meta))
}

func getIRODSOperation(meta *StagingMetadata) string {
	switch meta.Action {
	case ActionUpload:
		return fmt.Sprintf("PUT (%s)", meta.Path)
	case ActionRename:
		return "RENAME (already synced)"
	case ActionDelete:
		return fmt.Sprintf("DELETE (%s)", meta.Path)
	default:
		return "unknown"
	}
}

func printSimulatorStatus(sm *StagingStateManager) {
	all := sm.GetAll()
	if len(all) == 0 {
		fmt.Println("📭 Current state: empty")
		return
	}

	fmt.Println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
	fmt.Println("📊 Current state:")
	fmt.Println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
	for path, meta := range all {
		fmt.Printf("• %s\n", path)
		fmt.Printf("  ├─ Action: %s, IsNew: %v\n", meta.Action, meta.IsNew)
		fmt.Printf("  └─ iRODS: %s\n", getIRODSOperation(meta))
	}
	fmt.Println()
}
