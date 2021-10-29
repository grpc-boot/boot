package boot

import (
	"errors"
	"fmt"
	"hash/crc32"
	"math"
	"strconv"
)

var (
	ErrIpV4Address = errors.New(`invalid ip v4 address`)
)

func Long2Ip(ipVal uint32) string {
	return fmt.Sprintf("%d.%d.%d.%d", ipVal>>24, ipVal<<8>>24, ipVal<<16>>24, ipVal<<24>>24)
}

func Ip2Long(ip string) (ipVal uint32, err error) {
	var (
		val     uint32
		start   = 0
		leftMax = 4 * 8
	)

	for index, ch := range ip {
		if ch >= '0' && ch <= '9' {
			continue
		}

		if ch == '.' && start != index && (index-start < 4) {
			for i := index - start; i > 0; i-- {
				val += uint32(ip[index-i]-'0') * uint32(math.Pow(10, float64(i-1)))
			}
			if val > 0xff {
				return 0, ErrIpV4Address
			}

			leftMax -= 8
			if leftMax < 8 {
				return 0, ErrIpV4Address
			}

			ipVal += val << leftMax
			start = index + 1
			val = 0
			continue
		}

		return 0, ErrIpV4Address
	}

	//长度过长或过短判断
	if leftMax != 8 {
		return 0, ErrIpV4Address
	}

	for i := len(ip) - start; i > 0; i-- {
		val += uint32(ip[len(ip)-i]-'0') * uint32(math.Pow(10, float64(i-1)))
	}
	if val > 0xff {
		return 0, ErrIpV4Address
	}
	ipVal += val

	return ipVal, nil
}

func Bytes2Int64(data []byte) (num int64) {
	num, _ = strconv.ParseInt(string(data), 10, 64)
	return num
}

func Bytes2Uint64(data []byte) (num uint64) {
	num, _ = strconv.ParseUint(string(data), 10, 64)
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
