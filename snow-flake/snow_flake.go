package snow_flake

import (
	"errors"
	"sync"
	"time"
)

const (
	ModeWait    = 1
	ModeMaxTime = 2
	ModeError   = 3
)

var (
	ErrOutOfRange = errors.New("out of range")
	ErrTimeBack   = errors.New("time go back")
	ErrMachineId  = errors.New("illegal machine id")
	ErrLogicId    = errors.New("illegal logic id")
)

//1位0，41位毫秒时间戳，8位机器码，2位业务码，12位递增值
type SnowFlake struct {
	mode           uint8
	lastTimeStamp  int64
	timeStampBegin int64
	index          int16
	machId         int64
	step           work
	mutex          sync.Mutex
}

type work func() (err error)

func New(mode uint8, id uint8, timeStampBegin int64) (sf *SnowFlake, err error) {
	if id > 0xff {
		return nil, ErrMachineId
	}

	sf = &SnowFlake{
		mode:           mode,
		lastTimeStamp:  time.Now().UnixNano() / 1e6,
		timeStampBegin: timeStampBegin,
		machId:         int64(id) << 14,
	}

	switch mode {
	case ModeMaxTime:
		sf.step = sf.max
	case ModeError:
		sf.step = sf.err
	default:
		sf.step = sf.wait
	}
	return
}

func (sf *SnowFlake) Id(logicId uint8) (int64, error) {
	if logicId > 3 {
		return 0, ErrLogicId
	}

	sf.mutex.Lock()
	defer sf.mutex.Unlock()

	err := sf.step()
	if err != nil {
		return 0, err
	}

	return ((sf.lastTimeStamp - sf.timeStampBegin) << 22) + sf.machId + (int64(logicId) << 12) + int64(sf.index), nil
}

func (sf *SnowFlake) Info(id int64) (timestamp int64, machineId uint8, logicId uint8, index int16) {
	timestamp = (id >> 22) + sf.timeStampBegin
	machineId = uint8((id >> 14) & 0xff)
	logicId = uint8((id >> 12) & 3)
	index = int16(id & 0xfff)
	return
}

func (sf *SnowFlake) wait() (err error) {
	curTimeStamp := time.Now().UnixNano() / 1e6

	//时钟回拨等待处理
	for curTimeStamp < sf.lastTimeStamp {
		time.Sleep(time.Millisecond * 5)
		curTimeStamp = time.Now().UnixNano() / 1e6
	}

	if curTimeStamp == sf.lastTimeStamp {
		sf.index++
		if sf.index > 0xfff {
			return ErrOutOfRange
		}
	} else {
		sf.index = 0
		sf.lastTimeStamp = curTimeStamp
	}
	return nil
}

func (sf *SnowFlake) max() (err error) {
	curTimeStamp := time.Now().UnixNano() / 1e6

	//时钟回拨使用最大时间
	if curTimeStamp < sf.lastTimeStamp {
		curTimeStamp = sf.lastTimeStamp
	}

	if curTimeStamp == sf.lastTimeStamp {
		sf.index++
		if sf.index > 0xfff {
			return ErrOutOfRange
		}
	} else {
		sf.index = 0
		sf.lastTimeStamp = curTimeStamp
	}
	return nil
}

func (sf *SnowFlake) err() (err error) {
	curTimeStamp := time.Now().UnixNano() / 1e6
	//时钟回拨直接抛出异常
	if curTimeStamp < sf.lastTimeStamp {
		return ErrTimeBack
	}

	if curTimeStamp == sf.lastTimeStamp {
		sf.index++
		if sf.index > 0xfff {
			return ErrOutOfRange
		}
	} else {
		sf.index = 0
		sf.lastTimeStamp = curTimeStamp
	}
	return nil
}
