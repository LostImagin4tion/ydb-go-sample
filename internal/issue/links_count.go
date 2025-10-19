package issue

import "github.com/google/uuid"

type LinksCount struct {
	Id         uuid.UUID `sql:"id"`
	LinksCount uint64    `sql:"links_count"`
}
