package dorm

import (
	"crypto/rand"
	"math"
	mathRand "math/rand"
)

// NewRandomNonZeroInt returns a random non-zero integer [0,math.MaxInt64)
func NewRandomNonZeroInt() int64 {
	return mathRand.Int63n(math.MaxInt64)
}

// NewRandomBool returns a random boolean
func NewRandomBool() bool {
	return mathRand.Intn(2) == 0
}

// NewRandomEngStr returns a random English string
func NewRandomEngStr(digit int) (string, error) {
	const letters = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"

	b := make([]byte, digit)
	_, err := rand.Read(b)
	if err != nil {
		return "", err
	}

	var result string
	for _, v := range b {
		result += string(letters[int(v)%len(letters)])
	}

	return result, nil
}
