package container

//golang空结构不占用空间
var itemExists = struct{}{}

//非线程安全版本，如需要线程安全版本，需要自行加锁
//注意不同类型的同一个值是不同的item，如a int64 = 8 与 b int32 = 8 不是同一个值
type Set struct {
	items map[interface{}]struct{}
}

func NewSet(values ...interface{}) *Set {
	set := &Set{items: make(map[interface{}]struct{}, len(values))}
	if len(values) > 0 {
		set.Add(values...)
	}
	return set
}

func (set *Set) Add(items ...interface{}) {
	for _, item := range items {
		set.items[item] = itemExists
	}
}

func (set *Set) Remove(items ...interface{}) {
	for _, item := range items {
		delete(set.items, item)
	}
}

func (set *Set) Contains(items ...interface{}) bool {
	for _, item := range items {
		if _, contains := set.items[item]; !contains {
			return false
		}
	}
	return true
}

func (set *Set) Empty() bool {
	return set.Size() == 0
}

func (set *Set) Size() int {
	return len(set.items)
}

func (set *Set) Clear() {
	set.items = make(map[interface{}]struct{})
}
