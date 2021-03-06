package client

import (
	"sync"
	"time"

	"github.com/MagalixCorp/magalix-agent/proto"
	"github.com/MagalixCorp/magalix-agent/utils"
	"github.com/kovetskiy/lorg"
	structured "github.com/reconquest/cog"
	"github.com/reconquest/karma-go"
)

var _ structured.Sender = ((*Client)(nil)).sendLogs

func (client *Client) sendLogs(
	level lorg.Level, hierarchy karma.Hierarchical,
) error {
	client.logsQueue <- proto.PacketLogItem{
		Level: level,
		Date:  time.Now().UTC(),
		Data:  hierarchy.String(),
	}

	return nil
}

func (client *Client) initLogger() {
	client.Logger = client.parentLogger.NewChild()

	// instead of default displayer we will display to stderr using global
	// logger.
	// Note that parentLogger is the global stderr
	client.Logger.SetDisplayer(client.parentLogger.Display)

	if client.shouldSendLogs {
		// as sender we will use client's packet logs
		client.Logger.SetSender(client.sendLogs)
		client.initLogsQueue()
	}

	client.Logger.Log.SetExiter(func(int) {
		return
	})
}

func (client *Client) initLogsQueue() {
	client.logsQueue = make(chan proto.PacketLogItem, logsQueueSize)
	client.logsQueueWorker = &sync.WaitGroup{}

	go client.watchLogsQueue()
}

func (client *Client) watchLogsQueue() {
	client.logsQueueWorker.Add(1)
	defer client.logsQueueWorker.Done()

	client.parentLogger.Trace("{logs:queue} logs queue watcher started")

	var fatal bool

	// retry for 5 times then drop the packet

	for {
		logs := proto.PacketLogs{}
		t := time.Now()
		for {
			select {
			case log := <-client.logsQueue:
				logs = append(logs, log)
				if !fatal {
					fatal = log.Level == lorg.LevelFatal
				}
				if fatal || time.Now().Sub(t) > time.Minute {
					goto flush
				}
			case <-time.After(time.Minute):
				goto flush
			}
		}

	flush:

		if client.shouldSendLogs {
			client.parentLogger.Tracef(nil, "sending %v log entries", len(logs))
			client.Pipe(Package{
				Kind:        proto.PacketKindLogs,
				ExpiryTime:  utils.After(10 * time.Minute),
				ExpiryCount: 2,
				Priority:    9,
				Retries:     4,
				Data:        logs,
			})

			if fatal {
				client.Done(1)
				goto done
			}
		}

	}
done:
}
