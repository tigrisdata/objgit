package kube

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"

	"sigs.k8s.io/yaml"
)

// DecodeYAMLStream reads a multi-document YAML stream, such as kustomize
// output, and returns one Object per document. Documents that hold only
// comments or whitespace are skipped. An empty stream returns no objects and
// no error; the caller decides whether that is a failure.
//
// Documents split on a line that starts with "---", as in apimachinery's
// YAMLReader. Each document converts to JSON the way kubectl converts it, and
// integers keep their full precision.
func DecodeYAMLStream(r io.Reader) ([]Object, error) {
	docs, err := splitYAML(r)
	if err != nil {
		return nil, err
	}
	var objs []Object
	for i, doc := range docs {
		data, err := yaml.YAMLToJSONStrict(doc)
		if err != nil {
			return nil, fmt.Errorf("document %d: %w", i+1, err)
		}
		data = bytes.TrimSpace(data)
		if bytes.Equal(data, []byte("null")) {
			continue
		}
		if len(data) == 0 || data[0] != '{' {
			return nil, fmt.Errorf("document %d is not a mapping", i+1)
		}
		dec := json.NewDecoder(bytes.NewReader(data))
		dec.UseNumber()
		var obj Object
		if err := dec.Decode(&obj); err != nil {
			return nil, fmt.Errorf("document %d: %w", i+1, err)
		}
		objs = append(objs, obj)
	}
	return objs, nil
}

// splitYAML splits a stream into documents. A separator line is "---"
// followed by nothing, whitespace, or a comment. Other content after "---",
// such as "--- {a: 1}", is an error: the YAML converter would read only the
// first document of such a piece and drop the rest without an error.
func splitYAML(r io.Reader) ([][]byte, error) {
	var docs [][]byte
	var cur bytes.Buffer
	br := bufio.NewReader(r)
	for {
		line, err := br.ReadBytes('\n')
		if len(line) > 0 {
			if rest, ok := bytes.CutPrefix(line, []byte("---")); ok {
				if rest = bytes.TrimSpace(rest); len(rest) != 0 && rest[0] != '#' {
					return nil, fmt.Errorf("document %d: invalid document separator %q; put the content on the next line", len(docs)+2, bytes.TrimRight(line, "\r\n"))
				}
				docs = append(docs, bytes.Clone(cur.Bytes()))
				cur.Reset()
				line = nil
			}
			cur.Write(line)
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
	}
	return append(docs, cur.Bytes()), nil
}
