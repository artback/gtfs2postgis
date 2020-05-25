package time

import (
	"fmt"
	"strconv"
	"strings"
)

type DateString = string

func AddHoursToTimeString(time string, sep string, hours int) string {
	parts := strings.Split(time, sep)
	var intParts []int
	for i, _ := range parts {
		val, err := strconv.Atoi(parts[i])
		if err != nil {
			panic(err)
		}
		intParts = append(intParts, val)
	}
	if intParts[0] < 4 {
		intParts[0] = intParts[0] + hours
	}
	return fmt.Sprintf("%02d"+sep+"%02d"+sep+"%02d", intParts[0], intParts[1], intParts[2])
}
