package container

import (
	"testing"
)

func TestMap(t *testing.T) {
	m := NewMap()

	keyValue := map[string]interface{}{
		"user": map[string]interface{}{
			"id":   15,
			"name": "ddadf",
		},
		"listLength": 34,
		"key":        "value",
	}

	for key, value := range keyValue {
		m.Set(key, value)
	}

	if int64(len(keyValue)) != m.Length() {
		t.Fatalf("want %d, got %d", len(keyValue), m.Length())
	}

	val, exists := m.Get("user")
	if !exists {
		t.Fatalf("want true, got %t", exists)
	}

	if _, ok := val.(map[string]interface{}); !ok {
		t.Fatalf("want true, got %t", ok)
	}

	m.Delete("key")

	if exists = m.Exists("key"); exists {
		t.Fatalf("want false, got %t", exists)
	}

	if int64(len(keyValue)-1) != m.Length() {
		t.Fatalf("want %d, got %d", len(keyValue)-1, m.Length())
	}
}

func TestSet(t *testing.T) {
	hs := NewSet()

	list := []interface{}{
		"12",
		12,
		int64(12),
	}
	for _, val := range list {
		hs.Add(val)
	}

	if hs.Size() != len(list) {
		t.Fatalf("want %d, got %d", len(list), hs.Size())
	}

	//int 12已经存在
	hs.Add(12)
	if hs.Size() != len(list) {
		t.Fatalf("want %d, got %d", len(list), hs.Size())
	}

	hs.Remove(12)
	if hs.Contains(12) {
		t.Fatalf("want false, got %t", hs.Contains(12))
	}

	if !hs.Contains(int64(12)) {
		t.Fatalf("want true, got %t", hs.Contains(int64(12)))
	}
}
