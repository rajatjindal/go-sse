package net

import (
	"bufio"
	"fmt"
	"io"
	"net/http"
	neturl "net/url"
	"strings"
	"sync"

	"github.com/google/uuid"
)

type Subscription interface {
	ErrFeed() <-chan error
	Feed() <-chan Event
	EventType() string
	Close()
}

type subscription struct {
	id      string
	parent  *sseFeed
	feed    chan Event
	errFeed chan error

	eventType string
}

func (s *subscription) ErrFeed() <-chan error {
	return s.errFeed
}

func (s *subscription) Feed() <-chan Event {
	return s.feed
}

func (s *subscription) EventType() string {
	return s.eventType
}

func (s *subscription) Close() {
	s.parent.closeSubscription(s.id)
}

type SSEFeed interface {
	Subscribe(eventType string) (Subscription, error)
	Close()
}

type sseFeed struct {
	subscriptions    map[string]*subscription
	subscriptionsMtx sync.Mutex

	stopChan        chan interface{}
	closed          bool
	unfinishedEvent *StringEvent
}

func ConnectWithSSEFeed(url string, headers map[string][]string) (SSEFeed, error) {
	parsedURL, err := neturl.Parse(url)
	if err != nil {
		return nil, err
	}

	req := &http.Request{
		Method: http.MethodGet,
		URL:    parsedURL,
		Header: headers,
	}

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}

	reader := bufio.NewReader(resp.Body)

	feed := &sseFeed{
		subscriptions: make(map[string]*subscription),
		stopChan:      make(chan interface{}),
	}

	go func(response *http.Response, feed *sseFeed) {
		defer response.Body.Close()
	loop:
		for {
			select {
			case <-feed.stopChan:
				break loop
			default:
				b, err := reader.ReadBytes('\n')
				if len(b) == 0 {
					continue
				}

				if err != nil && err != io.EOF {
					feed.error(err)
					return
				}
				feed.processRaw(b)
			}
		}
	}(resp, feed)

	return feed, nil
}

func (s *sseFeed) Close() {
	close(s.stopChan)
	for subId, _ := range s.subscriptions {
		s.closeSubscription(subId)
	}
	s.closed = true
}

func (s *sseFeed) Subscribe(eventType string) (Subscription, error) {
	if s.closed {
		return nil, fmt.Errorf("sse feed closed")
	}

	sub := &subscription{
		id:        uuid.New().String(),
		parent:    s,
		eventType: eventType,
		feed:      make(chan Event),
		errFeed:   make(chan error, 1),
	}

	s.subscriptionsMtx.Lock()
	defer s.subscriptionsMtx.Unlock()

	s.subscriptions[sub.id] = sub

	return sub, nil
}

func (s *sseFeed) closeSubscription(id string) bool {
	s.subscriptionsMtx.Lock()
	defer s.subscriptionsMtx.Unlock()

	if sub, ok := s.subscriptions[id]; ok {
		close(sub.feed)
		return true
	}
	return false
}

func (s *sseFeed) processRaw(b []byte) {
	if len(b) == 1 && b[0] == '\n' {
		s.subscriptionsMtx.Lock()
		defer s.subscriptionsMtx.Unlock()

		// previous event is complete
		if s.unfinishedEvent == nil {
			return
		}
		evt := StringEvent{
			Id:    s.unfinishedEvent.Id,
			Event: s.unfinishedEvent.Event,
			Data:  s.unfinishedEvent.Data,
		}
		s.unfinishedEvent = nil
		for _, subscription := range s.subscriptions {
			if subscription.eventType == "" || subscription.eventType == evt.Event {
				subscription.feed <- evt
			}
		}
	}

	payload := strings.TrimRight(string(b), "\n")
	split := strings.SplitN(payload, ":", 2)

	// received comment or heartbeat
	if split[0] == "" {
		return
	}

	if s.unfinishedEvent == nil {
		s.unfinishedEvent = &StringEvent{}
	}

	switch split[0] {
	case "id":
		s.unfinishedEvent.Id = strings.Trim(split[1], " ")
	case "event":
		s.unfinishedEvent.Event = strings.Trim(split[1], " ")
	case "data":
		s.unfinishedEvent.Data = strings.Trim(split[1], " ")
	}
}

func (s *sseFeed) error(err error) {
	s.subscriptionsMtx.Lock()
	defer s.subscriptionsMtx.Unlock()

	for _, subscription := range s.subscriptions {
		subscription.errFeed <- err
	}

	s.Close()
}
