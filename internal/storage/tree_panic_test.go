package storage

import (
	"context"
	"strings"
	"testing"
)

func TestListObjectsTreeRecoversPrefixPanic(t *testing.T) {
	list := func(_ context.Context, prefix string, _ *string) ([]Object, []string, *string, error) {
		switch prefix {
		case "":
			return nil, []string{"a/", "b/"}, nil, nil
		case "a/":
			panic("boom")
		default:
			return []Object{obj(prefix + "k")}, nil, nil, nil
		}
	}
	var got []string
	_, err := listObjectsTree(context.Background(), "b", "", list, func(objs []Object) error {
		for _, o := range objs {
			got = append(got, o.Key)
		}
		return nil
	}, 2, 0, nil)

	if err == nil || !strings.Contains(err.Error(), "panic in list_prefix: boom") {
		t.Fatalf("err = %v, want recovered panic", err)
	}
	if len(got) != 1 || got[0] != "b/k" {
		t.Fatalf("keys = %v, want [b/k]: a panicking prefix must not abort the walk", got)
	}
}
