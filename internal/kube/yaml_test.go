package kube

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestDecodeYAMLStream(t *testing.T) {
	tests := []struct {
		name      string
		input     string
		wantNames []string
		wantErr   string // substring; "" means success
	}{
		{
			name:      "one document",
			input:     "apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: a\n",
			wantNames: []string{"a"},
		},
		{
			name:      "kustomize output",
			input:     "apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: a\n---\napiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: b\n",
			wantNames: []string{"a", "b"},
		},
		{
			name:      "leading and trailing separators",
			input:     "---\nmetadata:\n  name: a\n---\n",
			wantNames: []string{"a"},
		},
		{
			name:      "separator with a comment",
			input:     "metadata:\n  name: a\n--- # next\nmetadata:\n  name: b\n",
			wantNames: []string{"a", "b"},
		},
		{
			name:      "comment-only and empty documents are skipped",
			input:     "# header\n---\n\n---\nmetadata:\n  name: a\n---\n# trailer\n",
			wantNames: []string{"a"},
		},
		{
			name:      "three dashes inside a block scalar do not split",
			input:     "metadata:\n  name: a\ndata:\n  script: |\n    echo one\n    ---x\n",
			wantNames: []string{"a"},
		},
		{
			name:      "CRLF line endings",
			input:     "metadata:\r\n  name: a\r\n---\r\nmetadata:\r\n  name: b\r\n",
			wantNames: []string{"a", "b"},
		},
		{
			name:      "empty stream",
			input:     "",
			wantNames: nil,
		},
		{
			name:    "malformed YAML names the document",
			input:   "metadata:\n  name: a\n---\nmetadata: [unclosed\n",
			wantErr: "document 2",
		},
		{
			name:    "a list is not an object",
			input:   "- a\n- b\n",
			wantErr: "not a mapping",
		},
		{
			name:    "a scalar is not an object",
			input:   "hello\n",
			wantErr: "not a mapping",
		},
		{
			name:    "a separator with content is an error, not a lost document",
			input:   "metadata:\n  name: a\n--- {metadata: {name: b}}\n",
			wantErr: "document separator",
		},
		{
			name:    "a separator with a block scalar is an error",
			input:   "metadata:\n  name: a\n--- |\n  text\n",
			wantErr: "document separator",
		},
		{
			name:    "duplicate keys are rejected",
			input:   "metadata:\n  name: a\n  name: b\n",
			wantErr: "document 1",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			objs, err := DecodeYAMLStream(strings.NewReader(tt.input))
			if tt.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
					t.Fatalf("err = %v, want it to contain %q", err, tt.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("DecodeYAMLStream: %v", err)
			}
			var names []string
			for _, o := range objs {
				names = append(names, o.Name())
			}
			if strings.Join(names, ",") != strings.Join(tt.wantNames, ",") {
				t.Errorf("names = %q, want %q", names, tt.wantNames)
			}
		})
	}
}

func TestDecodeYAMLStreamKeepsLargeIntegers(t *testing.T) {
	objs, err := DecodeYAMLStream(strings.NewReader("spec:\n  big: 9007199254740993\n  ratio: 0.5\n"))
	if err != nil {
		t.Fatalf("DecodeYAMLStream: %v", err)
	}
	data, err := json.Marshal(objs[0])
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if want := `{"spec":{"big":9007199254740993,"ratio":0.5}}`; string(data) != want {
		t.Errorf("JSON = %s, want %s", data, want)
	}
}
