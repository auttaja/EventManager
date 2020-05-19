package eventmanager

import (
	"encoding/json"
	"fmt"
	cloudevents "github.com/cloudevents/sdk-go/v2"
	"github.com/google/uuid"
	"github.com/nats-io/nats.go"
)

type EventHandler func(m *cloudevents.Event)

type EventManager struct {
	Broker        *nats.Conn
	subscriptions map[string]*nats.Subscription
	app           string
}

func New(natsAddr string, app string) (*EventManager, error) {
	nc, err := nats.Connect(natsAddr)
	if err != nil {
		return nil, err
	}
	em := &EventManager{
		Broker:        nc,
		subscriptions: make(map[string]*nats.Subscription),
		app:           app,
	}

	return em, nil
}

func natsWrap(f EventHandler) nats.MsgHandler {
	return func(m *nats.Msg) {
		evt := cloudevents.NewEvent()
		if err := json.Unmarshal(m.Data, &evt); err != nil {
			return
		}

		f(&evt)
		return
	}
}

func (m *EventManager) Listen(subject string, cb EventHandler) error {
	sub, err := m.Broker.Subscribe(subject, natsWrap(cb))
	if err != nil {
		return err
	}

	m.subscriptions[subject] = sub
	return nil
}

func (m *EventManager) Publish(subject, t string, data interface{}) error {
	evt := cloudevents.NewEvent()
	u, err := uuid.NewUUID()
	if err != nil {
		return err
	}
	evt.SetID(u.String())
	evt.SetSource(fmt.Sprintf("auttaja.io/%s", m.app))
	evt.SetType(t)
	if err := evt.SetData(cloudevents.ApplicationJSON, data); err != nil {
		return err
	}

	bytes, err := json.Marshal(evt)
	if err != nil {
		return err
	}

	if err := m.Broker.Publish(subject, bytes); err != nil {
		return err
	}

	return nil
}
