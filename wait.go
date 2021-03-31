package main

import "sync"

type WaitGroupWrapper struct {
	sync.WaitGroup
}

func (wg *WaitGroupWrapper) Wrap(f func()) {
	wg.Add(1)
	go func() {
		f()
		wg.Done()
	}()
}
