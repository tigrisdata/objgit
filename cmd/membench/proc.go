package main

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
)

// procStatus is the subset of /proc/<pid>/status and /proc/<pid>/smaps_rollup
// this harness records, in bytes. The kernel reports kB; parseProcKV converts.
//
// VmHWM is the number that matters for sizing: it is the high-water mark the
// kernel itself keeps, so it cannot be missed between two samples the way a
// spike in VmRSS can be.
type procStatus struct {
	VmRSS        uint64
	VmHWM        uint64
	Pss          uint64
	PrivateDirty uint64
}

// parseProcKV reads the "Key:\t   1234 kB" lines that both /proc/<pid>/status
// and /proc/<pid>/smaps_rollup use, and returns them in bytes. Lines that do
// not carry a kB count (Name, State, the smaps_rollup header) are skipped,
// because no caller wants them.
func parseProcKV(r io.Reader) (map[string]uint64, error) {
	out := map[string]uint64{}
	sc := bufio.NewScanner(r)
	for sc.Scan() {
		key, rest, ok := strings.Cut(sc.Text(), ":")
		if !ok {
			continue
		}
		fields := strings.Fields(rest)
		if len(fields) != 2 || fields[1] != "kB" {
			continue
		}
		n, err := strconv.ParseUint(fields[0], 10, 64)
		if err != nil {
			return nil, fmt.Errorf("membench: %q is not a kB count: %w", sc.Text(), err)
		}
		out[key] = n * 1024
	}
	if err := sc.Err(); err != nil {
		return nil, fmt.Errorf("membench: reading proc file: %w", err)
	}
	return out, nil
}

// readProcStatus samples one process. /proc/<pid>/status is required;
// smaps_rollup is best effort, because it needs a 4.14 or newer kernel and
// permission to read another process's mappings. Losing Pss costs detail, not
// the measurement.
func readProcStatus(pid int) (procStatus, error) {
	var ps procStatus

	kv, err := readProcFile(fmt.Sprintf("/proc/%d/status", pid))
	if err != nil {
		return ps, err
	}
	ps.VmRSS = kv["VmRSS"]
	ps.VmHWM = kv["VmHWM"]

	if rollup, err := readProcFile(fmt.Sprintf("/proc/%d/smaps_rollup", pid)); err == nil {
		ps.Pss = rollup["Pss"]
		ps.PrivateDirty = rollup["Private_Dirty"]
	}

	return ps, nil
}

func readProcFile(name string) (map[string]uint64, error) {
	f, err := os.Open(name)
	if err != nil {
		return nil, fmt.Errorf("membench: can't open %s: %w", name, err)
	}
	defer f.Close()

	return parseProcKV(f)
}
