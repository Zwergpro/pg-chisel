package actions

type Storage interface {
	Get(key string) []string
	GetSet(key string) map[string]struct{}
	Set(key string, values []string)
	Delete(key string)
}
