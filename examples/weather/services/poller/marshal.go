package poller

import (
	"bytes"
	"encoding/binary"
	"io"
)

// marshalCityAndState marshals a city and state into a byte slice using binary encoding.
func marshalCityAndState(city, state string) []byte {
	stateLen := len(state)
	cityLen := len(city)
	buf := new(bytes.Buffer)
	binary.Write(buf, binary.LittleEndian, int32(stateLen))
	buf.Write([]byte(state))
	binary.Write(buf, binary.LittleEndian, int32(cityLen))
	buf.Write([]byte(city))
	return buf.Bytes()
}

// unmarshalCityAndState unmarshals a city and state from a byte slice using binary encoding.
func unmarshalCityAndState(data []byte) (city, state string, err error) {
	buf := bytes.NewBuffer(data)
	var stateLen, cityLen int32
	err = binary.Read(buf, binary.LittleEndian, &stateLen)
	if err != nil {
		return
	}
	stateBytes := make([]byte, stateLen)
	_, err = io.ReadFull(buf, stateBytes)
	if err != nil {
		return
	}
	err = binary.Read(buf, binary.LittleEndian, &cityLen)
	if err != nil {
		return
	}
	cityBytes := make([]byte, cityLen)
	_, err = io.ReadFull(buf, cityBytes)
	if err != nil {
		return
	}
	state = string(stateBytes)
	city = string(cityBytes)
	return
}
