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

	log.Println("Creating schema...")

	schemaRepository.DropSchema()
	schemaRepository.CreateSchema()

	// ====== INSERT DATA ======
	log.Println("Inserting data...")

	firstIssue, err := issuesRepository.AddIssue("Ticket 1", "Author 1")
	if err != nil {
		log.Fatalf("Some error happened (1): %v\n", err)
	}

	secondIssue, err := issuesRepository.AddIssue("Ticket 2", "Author 2")
	if err != nil {
		log.Fatalf("Some error happened (2): %v\n", err)
	}

	thirdIssue, err := issuesRepository.AddIssue("Ticket 3", "Author 3")
	if err != nil {
		log.Fatalf("Some error happened (3): %v\n", err)
	}

	// ====== CHECK DATA ======
	log.Println("Checking data...")

	allIssues, err := issuesRepository.FindAll()
	if err != nil {
		log.Fatalf("Some error happened while find all: %v\n", err)
	}

	log.Println("All issues:")
	for _, issue := range allIssues {
		log.Printf("%v\n", issue)
	}

	first, err := issuesRepository.FindById(firstIssue.Id)
	if err != nil {
		log.Fatal(err)
	} else {
		log.Printf("First: %v\n", first)
	}

	second, err := issuesRepository.FindById(secondIssue.Id)
	if err != nil {
		log.Fatal(err)
	} else {
		log.Printf("Second: %v\n", second)
	}

	third, err := issuesRepository.FindById(thirdIssue.Id)
	if err != nil {
		log.Fatal(err)
	} else {
		log.Printf("Third: %v\n", second)
	}

	// ====== CHECK TRANSACTIONS ======
	log.Println("Checking non-interactive transaction...")

	result1, err := issuesRepository.LinkTicketsNoInteractive(first.Id, second.Id)
	if err != nil {
		log.Fatal(err)
	} else {
		log.Printf("Non-interactive transaction result: %v\n", result1)
	}

	result2, err := issuesRepository.LinkTicketsInteractive(second.Id, third.Id)
	if err != nil {
		log.Fatal(err)
	} else {
		log.Printf("Interactive transaction result: %v\n", result2)
	}

	// ====== CHECK DATA AGAIN ======
	log.Println("All issues:")

	allIssues, err = issuesRepository.FindAll()
	if err != nil {
		log.Fatalf("Some error happened while find all: %v\n", err)
	}

	for _, issue := range allIssues {
		log.Printf("%v\n", issue)
	}
}
