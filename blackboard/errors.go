package blackboard

import (
	"errors"
	"fmt"
)

// ErrBlackboard is the base of every library error; consumers match it with errors.Is.
var ErrBlackboard = errors.New("blackboard")

// ErrRevisionConflict reports a compare-and-swap put or delete that lost the revision race.
var ErrRevisionConflict = fmt.Errorf("%w: revision conflict", ErrBlackboard)

// ErrInvalidValue reports an empty key, a value that is not a JSON object, or one over the size limit.
var ErrInvalidValue = fmt.Errorf("%w: invalid value", ErrBlackboard)

// ErrExists reports a create for a blackboard that already exists.
var ErrExists = fmt.Errorf("%w: blackboard exists", ErrBlackboard)

// ErrNotFound reports an operation naming a blackboard that does not exist.
var ErrNotFound = fmt.Errorf("%w: blackboard not found", ErrBlackboard)

// ErrPreconditionParse reports a precondition expression that is empty or syntactically invalid.
var ErrPreconditionParse = fmt.Errorf("%w: precondition parse error", ErrBlackboard)

// ErrInvalidKey reports a history lookup given a wildcard instead of an exact key.
var ErrInvalidKey = errors.New("blackboard: history accepts an exact key, not a wildcard")

// ErrInvalidBlackboardID reports a blackboard id that does not match ^[a-zA-Z0-9-]+$.
var ErrInvalidBlackboardID = errors.New("blackboard: invalid blackboard id")

// ErrInvalidInstance reports a registry instance id that does not match ^[a-z0-9-]{1,32}$.
var ErrInvalidInstance = errors.New("blackboard: invalid instance")

// ErrInvalidTrack reports a registry track outside ^[a-z][a-z0-9-]{0,15}$ or bound without an instance.
var ErrInvalidTrack = errors.New("blackboard: invalid track")

// ErrAgentStarted reports a start on an agent that is already attached to a blackboard.
var ErrAgentStarted = fmt.Errorf("%w: agent already started", ErrBlackboard)
