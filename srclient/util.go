package srclient

import (
	"fmt"
	"math/rand"
	"net/url"
)

type pathParam string

func newPathParam(param interface{}) pathParam {
	if v, ok := param.(pathParam); ok {
		return v
	}

	return pathParam(fmt.Sprintf("%v", param))
}

func (p pathParam) String() string {
	return url.PathEscape(string(p))
}

type urlPath string

func (u urlPath) Format(params ...interface{}) urlPath {
	pathParams := []interface{}{}
	for _, param := range params {
		pathParams = append(pathParams, newPathParam(param))
	}

	return urlPath(fmt.Sprintf(string(u), pathParams...))
}

func enableOpt(opts []bool) bool {
	if len(opts) > 0 {
		return opts[0]
	}

	return true
}

func randomString(n int) string {
	var letters = []rune("abcdefghijklmnopqrstuvwxyz")

	s := make([]rune, n)
	for i := range s {
		s[i] = letters[rand.Intn(len(letters))]
	}
	return string(s)
}
