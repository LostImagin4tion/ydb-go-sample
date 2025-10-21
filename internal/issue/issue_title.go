package issue

import "github.com/google/uuid"

type IssueTitle struct {
	Id    uuid.UUID `sql:"id"`
	Title string    `sql:"title"`
}
