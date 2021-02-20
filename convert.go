package boot

import (
	"strconv"
)

func Bytes2Int64(data []byte) (num int64) {
	num, _ = strconv.ParseInt(string(data), 10, 64)
	return num
}
