// in order to debug easier, inspired by etcd and extended based on it
package raft

type Logger interface {
	//personal test mode ---> for single test instead of fmt.Println
	Test(...interface{})
	TestF(string, ...interface{})

	Debug(...interface{})
	DebugF(string, ...interface{})

	Fatal(...interface{})
	FatalF(string, ...interface{})

	Panic(...interface{})
	PanicF(string, ...interface{})
}
