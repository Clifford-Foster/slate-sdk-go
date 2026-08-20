// Contract: contracts/sidecar.md — the event-stream framing rules A24 and B10 share.

package bbsdk

import (
	"bufio"
	"io"
	"strings"
)

// The framing a frame is assembled from: the default event name, and the two fields the SDK reads.
// Every other field, id and retry among them, is skipped (rule S10).
const (
	defaultEventName = "message"
	fieldEvent       = "event"
	fieldData        = "data"
)

// sseFrame is one decoded server-sent event: its name and its joined data payload.
type sseFrame struct {
	event string
	data  string
}

// sseReader decodes the event stream the watch and the pull activation stream both speak (rule S10).
type sseReader struct {
	reader *bufio.Reader
}

// newSSEReader wraps a streaming response body as an event-stream decoder.
func newSSEReader(body io.Reader) *sseReader {
	return &sseReader{reader: bufio.NewReader(body)}
}

// next returns the next frame carrying data. Comment keepalives, unknown fields and frames with no
// data line produce no event, which is exactly why a stream carries no read deadline (rules S2, S10).
func (s *sseReader) next() (sseFrame, error) {
	event := ""
	var data []string
	for {
		line, err := s.readLine()
		if err != nil {
			return sseFrame{}, err
		}
		if line == "" {
			// A blank line closes the frame; one with no data line is not delivered.
			if len(data) == 0 {
				event = ""
				continue
			}
			name := event
			if name == "" {
				name = defaultEventName
			}
			return sseFrame{event: name, data: strings.Join(data, "\n")}, nil
		}
		if strings.HasPrefix(line, ":") {
			continue
		}
		field, value := splitSSEField(line)
		switch field {
		case fieldEvent:
			event = value
		case fieldData:
			data = append(data, value)
		}
	}
}

// readLine reads one line and drops its terminator; a stream ending mid-line delivers no frame.
func (s *sseReader) readLine() (string, error) {
	line, err := s.reader.ReadString('\n')
	if err != nil {
		return "", err
	}
	return strings.TrimSuffix(strings.TrimSuffix(line, "\n"), "\r"), nil
}

// splitSSEField splits a line into its field name and value, stripping one leading space after the colon.
func splitSSEField(line string) (field, value string) {
	colon := strings.IndexByte(line, ':')
	if colon < 0 {
		return line, ""
	}
	return line[:colon], strings.TrimPrefix(line[colon+1:], " ")
}
