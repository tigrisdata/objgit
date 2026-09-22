package main

import "testing"

func TestPickBuilds(t *testing.T) {
	t.Parallel()

	for _, tt := range []struct {
		name    string
		in      string
		want    []buildSpec
		wantErr bool
	}{
		{
			name: "the two sides of the format change",
			in:   "before,after",
			want: []buildSpec{{Name: "before", Ref: "v1.0.2"}, {Name: "after"}},
		},
		{
			name: "a named ref builds its own column",
			in:   "before,fixed=1bb4b3e",
			want: []buildSpec{{Name: "before", Ref: "v1.0.2"}, {Name: "fixed", Ref: "1bb4b3e"}},
		},
		{
			name: "an empty ref means the current checkout",
			in:   "working=",
			want: []buildSpec{{Name: "working"}},
		},
		{
			name: "whitespace is trimmed everywhere",
			in:   " before , fixed = 1bb4b3e ",
			want: []buildSpec{{Name: "before", Ref: "v1.0.2"}, {Name: "fixed", Ref: "1bb4b3e"}},
		},
		{
			name:    "a duplicate column name is refused",
			in:      "after,after",
			wantErr: true,
		},
		{
			name:    "two refs cannot share one column name",
			in:      "x=main,x=v1.0.2",
			wantErr: true,
		},
		{
			name:    "a bare word that is not a known side",
			in:      "sideways",
			wantErr: true,
		},
		{
			name:    "a ref with no name",
			in:      "=main",
			wantErr: true,
		},
		{
			name:    "nothing selected",
			in:      " , ",
			wantErr: true,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			// Not parallel: pickBuilds reads the -before-ref flag.
			*beforeRef = "v1.0.2"

			got, err := pickBuilds(tt.in)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("wanted an error, got %v", got)
				}
				return
			}
			if err != nil {
				t.Fatalf("pickBuilds: %v", err)
			}

			if len(got) != len(tt.want) {
				t.Fatalf("want %v, got %v", tt.want, got)
			}
			for i := range tt.want {
				if got[i].Name != tt.want[i].Name || got[i].Ref != tt.want[i].Ref {
					t.Errorf("column %d: want %+v, got %+v", i, tt.want[i], got[i])
				}
			}
		})
	}
}

func TestWorktreeName(t *testing.T) {
	t.Parallel()

	for _, tt := range []struct {
		in   string
		want string
	}{
		{"v1.0.2", "v1.0.2"},
		{"1bb4b3e", "1bb4b3e"},
		{"perf/shallow-lookup-cache", "perf-shallow-lookup-cache"},
		{"origin/main", "origin-main"},
	} {
		t.Run(tt.in, func(t *testing.T) {
			t.Parallel()

			if got := worktreeName(tt.in); got != tt.want {
				t.Logf("want: %s", tt.want)
				t.Logf("got:  %s", got)
				t.Error("a ref turned into a bad directory name")
			}
		})
	}
}

func TestScheduleInterleavesBuilds(t *testing.T) {
	t.Parallel()

	sides := []buildSpec{{Name: "before"}, {Name: "after"}}
	selected := []repoSpec{{Name: "small", Reps: 2}, {Name: "big", Reps: 1}}

	*reps = 0
	got := schedule(sides, selected)

	want := []cell{
		{Build: "before", Repo: "small", Rep: 1},
		{Build: "after", Repo: "small", Rep: 1},
		{Build: "before", Repo: "big", Rep: 1},
		{Build: "after", Repo: "big", Rep: 1},
		{Build: "before", Repo: "small", Rep: 2},
		{Build: "after", Repo: "small", Rep: 2},
	}

	if len(got) != len(want) {
		t.Fatalf("want %d cells, got %d: %+v", len(want), len(got), got)
	}
	for i := range want {
		if got[i].Build != want[i].Build || got[i].Repo != want[i].Repo || got[i].Rep != want[i].Rep {
			t.Fatalf("cell %d: want %+v, got %+v", i, want[i], got[i])
		}
	}
}
