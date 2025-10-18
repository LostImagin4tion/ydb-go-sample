package main

import (
	"log"
	"ydb-sample/internal/issue"
	"ydb-sample/internal/query"
	"ydb-sample/internal/schema"
)

func main() {
	var queryHelper = query.NewQueryHelper("grpc://localhost:2136/local")
	defer queryHelper.Close()

	var schemaRepository = schema.NewSchemaRepository(queryHelper)
	var issuesRepository = issue.NewIssueRepository(queryHelper)

	schemaRepository.DropSchema()
	schemaRepository.CreateSchema()

	firstIssue, err := issuesRepository.AddIssue("Ticket 1")
	if err != nil {
		log.Fatalf("Some error happened (1): %v\n", err)
	}
	
	_, err = issuesRepository.AddIssue("Ticket 2")
	if err != nil {
		log.Fatalf("Some error happened (2): %v\n", err)
	}

	_, err = issuesRepository.AddIssue("Ticket 3")
	if err != nil {
		log.Fatalf("Some error happened (3): %v\n", err)
	}

	issues, err := issuesRepository.FindAll()
	if err != nil {
		log.Fatalf("Some error happened while find all: %v\n", err)
	}
	for _, issue := range issues {
		log.Printf("Issue: %v\n", issue)
	}

	searchFirstIssue, err := issuesRepository.FindById(firstIssue.Id)
	if err != nil {
		log.Fatal(err)
	} else {
		log.Printf("First issue: %v\n", searchFirstIssue)
	}
}
