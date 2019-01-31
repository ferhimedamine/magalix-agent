package utils

import (
	"sync"
	"time"
)

type Ticker struct {
	name string

	interval time.Duration
	fn       func(tickTime time.Time)

	mutex *sync.Mutex

	waitChannels map[int64][]chan interface{}
	lastTick time.Time
}

func NewTicker(name string, interval time.Duration, fn func(time.Time)) *Ticker {
	return &Ticker{
		name:     name,
		interval: interval,
		fn:       fn,

		mutex: &sync.Mutex{},
		waitChannels: map[int64][]chan interface{}{},
	}
}

func (ticker *Ticker) nextTick() <-chan time.Time {
	interval := ticker.interval
	if time.Hour%interval == 0 {
		now := time.Now()
		// TODO: sub seconds
		nanos := time.Second*time.Duration(now.Second()) + time.Minute*time.Duration(now.Minute())
		next := interval - nanos%interval
		stderr.Infof(nil, "{%s ticker} next tick after %v", ticker.name, next)
		return time.After(next)
	}
	stderr.Infof(nil, "{%s ticker} next tick after interval %v", ticker.name, interval)
	return time.After(interval)
}

func (ticker *Ticker) unlockWaiting(tick time.Time) {
	ticker.mutex.Lock()
	defer ticker.mutex.Unlock()
	currentTickStamp := tick.Unix()
	for tickStamp, waitChannels := range ticker.waitChannels {
		if currentTickStamp >= tickStamp {
			for _, waitChan := range waitChannels {
				waitChan <- struct{}{}
				close(waitChan)
			}
			delete(ticker.waitChannels, tickStamp)
		}
	}
}

// Start start scanner
func (ticker *Ticker) Start(immediate, block bool) {
	tickerFn := func() {
		tick := ticker.nextTick()
		for {
			ticker.lastTick = <-tick

			ticker.fn(ticker.lastTick)

			// unlocks routines waiting for the next tick
			ticker.unlockWaiting(ticker.lastTick)
			tick = ticker.nextTick()
		}
	}

	if immediate {
		// block for first tick
		ticker.fn(time.Now())
	}
	if block {
		tickerFn()
	} else {
		go tickerFn()
	}
}

// WaitForNextTick returns a signal channel that gets unblocked after the next tick
// Example usage:
//  <- ticker.WaitForNextTick()
func (ticker *Ticker) WaitForNextTick() chan interface{} {
	return ticker.WaitForTick(ticker.lastTick.Add(ticker.interval))
}

func (ticker *Ticker) WaitForTick(tick time.Time) chan interface{} {
	ticker.mutex.Lock()
	defer ticker.mutex.Unlock()
	waitChan := make(chan interface{})
	var waitChannels []chan interface{}
	waitChannels, ok := ticker.waitChannels[tick.Unix()]
	if !ok {
		waitChannels = make([]chan interface{}, 0)
	}
	waitChannels = append(waitChannels, waitChan)
	ticker.waitChannels[tick.Unix()] = waitChannels

	return waitChan
}
