package cel_extensions

import (
	"github.com/google/cel-go/cel"
	"github.com/google/cel-go/common/types"
	"github.com/google/cel-go/common/types/ref"
)

const (
	PG_NULL = "\\N"
)

type Storage interface {
	Get(key string) []string
	GetSet(key string) map[string]struct{}
}

func NewEnv(opts ...cel.EnvOption) (*cel.Env, error) {
	opts = append(opts,
		cel.Constant("NULL", cel.StringType, types.String(PG_NULL)),
		cel.Variable("table", cel.MapType(cel.StringType, cel.BytesType)),
	)
	return cel.NewEnv(opts...)
}

func GetArrayFunc(storage Storage) cel.EnvOption {
	return cel.Function("array",
		cel.Overload("store_array_strings",
			[]*cel.Type{cel.StringType},
			cel.AnyType,
			cel.FunctionBinding(func(args ...ref.Val) ref.Val {
				if len(args) != 1 || args[0].Type() != types.StringType {
					return types.NewErr("array() expects a single string argument")
				}

				key := args[0].Value().(string)
				return types.DefaultTypeAdapter.NativeToValue(storage.Get(key))
			}),
		),
	)
}

func GetSetFunc(storage Storage) cel.EnvOption {
	return cel.Function("set",
		cel.Overload("store_set_strings",
			[]*cel.Type{cel.StringType},
			cel.AnyType,
			cel.FunctionBinding(func(args ...ref.Val) ref.Val {
				if len(args) != 1 || args[0].Type() != types.StringType {
					return types.NewErr("set() expects a single string argument")
				}

				key := args[0].Value().(string)
				return types.DefaultTypeAdapter.NativeToValue(storage.GetSet(key))
			}),
		),
	)
}
