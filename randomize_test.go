package dorm

import (
	"fmt"
	"math"
	"reflect"
)

// RandomizeDDBStruct Strの各パラメーターはprimitiveかつnot pointerであること(ただしSliceはOK)
// ref: https://github.com/volatiletech/randomize
func RandomizeDDBStruct(str interface{}) error {
	// Check if it's pointer
	value := reflect.ValueOf(str)
	kind := value.Kind()
	if kind != reflect.Ptr {
		return fmt.Errorf("Outer element should be a pointer, given a non-pointer: %T", str)
	}

	// Check if it's a struct
	value = value.Elem()
	kind = value.Kind()
	if kind != reflect.Struct {
		return fmt.Errorf("Inner element should be a struct, given a non-struct: %T", str)
	}

	nFields := value.NumField()

	// Iterate through fields, randomizing
	for i := 0; i < nFields; i++ {
		var val interface{}
		var err error

		fieldVal := value.Field(i)

		typ := fieldVal.Type()

		if val, err = randomizeField(fieldVal); err != nil {
			return err
		}

		newValue := reflect.ValueOf(val)

		if val == nil {
			// If the value is nil, we don't set it
			continue
		}

		if reflect.TypeOf(val) != typ {
			if reflect.TypeOf(val).Kind() == reflect.Slice || reflect.TypeOf(val).Kind() == reflect.Array {
				elemtyp := fieldVal.Type()
				slice := reflect.MakeSlice(elemtyp, newValue.Len(), newValue.Len())
				for i := 0; i < newValue.Len(); i++ {
					slice.Index(i).Set(newValue.Index(i))
				}

				newValue = slice
			} else {
				newValue = newValue.Convert(typ)
			}
		}

		fieldVal.Set(newValue)
	}

	return nil
}

func randomizeField(val reflect.Value) (resp interface{}, err error) {
	kind := val.Kind()

	switch kind {
	case reflect.String:
		resp, err = NewRandomEngStr(int(NewRandomNonZeroInt()%math.MaxInt8 + 10))
	case reflect.Bool:
		resp = NewRandomBool()
	case reflect.Int:
		resp = int(NewRandomNonZeroInt() % math.MaxInt32)
	case reflect.Int8:
		resp = int8(NewRandomNonZeroInt() % math.MaxInt8)
	case reflect.Int16:
		resp = int16(NewRandomNonZeroInt() % math.MaxInt16)
	case reflect.Int32:
		resp = int32(NewRandomNonZeroInt() % math.MaxInt32)
	case reflect.Int64:
		resp = NewRandomNonZeroInt()
	case reflect.Uint:
		resp = uint(NewRandomNonZeroInt() % math.MaxUint32)
	case reflect.Uint8:
		resp = uint8(NewRandomNonZeroInt() % math.MaxUint8)
	case reflect.Uint16:
		resp = uint16(NewRandomNonZeroInt() % math.MaxUint16)
	case reflect.Uint32:
		resp = uint32(NewRandomNonZeroInt() % math.MaxUint32)
	case reflect.Uint64:
		resp = uint64(NewRandomNonZeroInt())
	case reflect.Float32:
		resp = float32(float32(NewRandomNonZeroInt()%10)/10.0 + float32(NewRandomNonZeroInt()%10))
	case reflect.Float64:
		resp = float64(float64(NewRandomNonZeroInt()%10)/10.0 + float64(NewRandomNonZeroInt()%10))
	case reflect.Slice, reflect.Array:
		var slice []interface{}
		for i := 0; i < val.Len(); i++ {
			var elem interface{}
			if elem, err = randomizeField(val.Index(i)); err != nil {
				return nil, err
			}
			slice = append(slice, elem)
		}
		resp = slice
	case reflect.Interface:
		// nothing to do
	default:
		return nil, fmt.Errorf("Unsupported type: %v", kind)
	}

	if err != nil {
		return nil, err
	}

	return resp, nil
}
