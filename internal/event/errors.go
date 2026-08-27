package event

import (
	"errors"
)

var ErrorDuplicateEvent = errors.New("event_id already exists")