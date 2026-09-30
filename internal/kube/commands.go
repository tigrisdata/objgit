package kube

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"strings"

	"github.com/Xe/kefka/command"
	"github.com/Xe/kefka/command/registry"
	"mvdan.cc/sh/v3/interp"
)

// Origin is the update that a shell runs for, as the daemon knows it. The
// commands take it from the daemon and not from the OBJGIT_* variables,
// because a script can change those. The audit log and the PipelineRun
// metadata are the only record of which repository changed the cluster.
type Origin struct {
	Repo   string // the repository path
	Ref    string // the full ref name
	Branch string // the short branch name; "" when Ref is not a branch
	Commit string // the commit hash
}

// Register adds kube:apply and tekton:pipelinerun to reg, for the update
// origin. A nil client registers stubs that explain how to turn the commands
// on, so a hook that calls them fails with a reason instead of "command not
// found".
func Register(reg *registry.Impl, c *Client, origin Origin) {
	if c == nil {
		reg.Register("kube:apply", disabled("kube:apply"))
		reg.Register("tekton:pipelinerun", disabled("tekton:pipelinerun"))
		return
	}
	reg.Register("kube:apply", ApplyCommand{Client: c, Origin: origin})
	reg.Register("tekton:pipelinerun", PipelineRunCommand{Client: c, Origin: origin})
}

type disabled string

func (d disabled) Exec(_ context.Context, ec *command.ExecContext, _ []string) error {
	fmt.Fprintf(ec.Stderr, "%s: Kubernetes commands are disabled; start objgitd with -allow-kubernetes to enable them\n", string(d))
	return interp.ExitStatus(1)
}

// ApplyCommand is kube:apply. It reads a multi-document YAML stream from
// standard input and applies each object with Server-Side Apply, in stream
// order. The whole stream is parsed and checked before the first request. The
// first failed request stops the command. It never prunes.
type ApplyCommand struct {
	Client *Client
	Origin Origin
}

// Exec runs kube:apply.
func (a ApplyCommand) Exec(ctx context.Context, ec *command.ExecContext, args []string) error {
	const name = "kube:apply"
	if len(args) != 0 {
		fmt.Fprintln(ec.Stderr, "usage: kube:apply < manifests.yaml")
		return interp.ExitStatus(2)
	}
	in := ec.Stdin
	if in == nil {
		in = strings.NewReader("")
	}
	objs, err := DecodeYAMLStream(in)
	if err != nil {
		return fail(ec, name, "standard input: %v", err)
	}
	if len(objs) == 0 {
		return fail(ec, name, "no objects on standard input")
	}
	for i, obj := range objs {
		if err := check(obj, false); err != nil {
			return fail(ec, name, "object %d: %v", i+1, err)
		}
	}

	s := a.Client.Session()
	for _, obj := range objs {
		res, err := s.Apply(ctx, obj)
		if err != nil {
			return fail(ec, name, "%s: %v", describe(res.Resource, obj), err)
		}
		audit(a.Origin, "kube: applied", res)
		fmt.Fprintf(ec.Stdout, "%s serverside-applied\n", describe(res.Resource, res.Object))
	}
	return nil
}

// describe names obj the way kubectl does, such as
// "pipeline.tekton.dev/build". Before discovery succeeds, res is zero, so the
// kind stands in.
func describe(res Resource, obj Object) string {
	if res.Kind == "" {
		return strings.ToLower(obj.Kind()) + "/" + obj.Name()
	}
	return res.String() + "/" + obj.Name()
}

// fail writes "name: message" to stderr and returns exit status 1.
func fail(ec *command.ExecContext, name, format string, a ...any) error {
	fmt.Fprintf(ec.Stderr, name+": "+format+"\n", a...)
	return interp.ExitStatus(1)
}

// audit logs one change to the cluster, with the update that made it.
func audit(origin Origin, msg string, res Result) {
	slog.Info(msg,
		"repo", origin.Repo,
		"ref", origin.Ref,
		"sha", origin.Commit,
		"resource", res.Resource.String(),
		"namespace", res.Object.Namespace(),
		"name", res.Object.Name(),
	)
}

// readAll is io.ReadAll that closes r.
func readAll(r io.ReadCloser) ([]byte, error) {
	defer r.Close()
	return io.ReadAll(r)
}
