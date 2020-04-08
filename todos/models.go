package todos

import "time"

type Todo struct {
	ID         string `json:"ID,omitempty"`
	Datetime   time.Time
	Text       string
	IsComplete bool
}
