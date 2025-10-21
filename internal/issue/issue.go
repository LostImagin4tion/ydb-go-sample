package issue

import (
	"time"

	"github.com/google/uuid"
)

type Issue struct {
	Id         uuid.UUID `sql:"id"`
	Title      string    `sql:"title"`
	Timestamp  time.Time `sql:"created_at"`
	Author     string    `sql:"author"`
	LinksCount uint64    `sql:"links_count"`
	Status     string    `sql:"status"`
}
