package fluent

import (
	"fmt"
	"io"
	"net"
	"strconv"
	"time"
)

// ErrNoConnection is returned when you try to send but there is no connection or not connection can be established.
var ErrNoConnection = fmt.Errorf("no connection")

// Config can be used to configure the logger.
type Config struct {
	FluentPort int
	FluentHost string
	Timeout    time.Duration
}

// Fluent is a connection to Fluentd
type Fluent struct {
	Config
	conn io.WriteCloser
}

// New creates a new Logger.
func New(config Config) *Fluent {
	if config.FluentHost == "" {
		config.FluentHost = "127.0.0.1"
	}
	if config.FluentPort == 0 {
		config.FluentPort = 24224
	}
	if config.Timeout == 0 {
		config.Timeout = 2 * time.Second
	}
	return &Fluent{Config: config}
}

func (f *Fluent) Send(data []byte) error {
	return f.send(data)
}

func (f *Fluent) Encode(tag string, tm time.Time, message interface{}) (data []byte, err error) {
	timeUnix := tm.Unix()
	msg := &Message{Tag: tag, Time: timeUnix, Record: message}
	data, err = msg.MarshalMsg(nil)
	return
}

// Close closes the connection.
func (f *Fluent) Close() error {
	if f.conn != nil {
		err := f.conn.Close()
		f.conn = nil
		return err
	}
	return nil
}

// connect establishes a new connection using the specified transport.
func (f *Fluent) connect() (err error) {
	f.conn, err = net.DialTimeout("tcp", f.Config.FluentHost+":"+strconv.Itoa(f.Config.FluentPort), f.Config.Timeout)
	return
}

func (f *Fluent) send(data []byte) error {
	if f.conn == nil {
		if err := f.connect(); err != nil {
			return ErrNoConnection
		}
	}
	_, err := f.conn.Write(data)
	return err
}
