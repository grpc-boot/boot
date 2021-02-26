package boot

import (
	"fmt"
	"hash/crc32"
	"strconv"
)

func Bytes2Int64(data []byte) (num int64) {
	num, _ = strconv.ParseInt(string(data), 10, 64)
	return num
}

func HashOrNumber(key interface{}) (value uint32) {
	switch key.(type) {
	case uint8:
		return uint32(key.(uint8))
	case uint16:
		return uint32(key.(uint16))
	case uint32:
		return key.(uint32)
	case uint64:
		return uint32(key.(uint64))
	case uint:
		return uint32(key.(uint))
	case int8:
		return uint32(key.(int8))
	case int16:
		return uint32(key.(int16))
	case int32:
		return uint32(key.(int32))
	case int64:
		return uint32(key.(int64))
	case int:
		return uint32(key.(int))
	case CanHash:
		return key.(CanHash).HashCode()
	case string:
		return crc32.ChecksumIEEE([]byte(key.(string)))
	case []byte:
		return crc32.ChecksumIEEE(key.([]byte))
	}

	return crc32.ChecksumIEEE([]byte(fmt.Sprintln(key)))
}
