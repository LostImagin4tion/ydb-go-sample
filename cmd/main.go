package main

import (
	"context"
	"log"
	"ydb-sample/internal/issue"
	"ydb-sample/internal/query"
	"ydb-sample/internal/schema"
	"ydb-sample/internal/topic"

	"github.com/google/uuid"
)

func main() {
	var ctx, cancel = context.WithCancel(context.Background())
	defer cancel()

	var queryHelper = query.NewQueryHelper(ctx, "grpc://localhost:2136/local")
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
	}
	log.Printf("First: %v\n", first)

	second, err := issuesRepository.FindById(secondIssue.Id)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("Second: %v\n", second)

	third, err := issuesRepository.FindById(thirdIssue.Id)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("Third: %v\n", second)

	// ====== CHECK TRANSACTIONS ======
	log.Println("Checking non-interactive transaction...")

	result1, err := issuesRepository.LinkTicketsNoInteractive(first.Id, second.Id)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("Non-interactive transaction result: %v\n", result1)

	result2, err := issuesRepository.LinkTicketsInteractive(second.Id, third.Id)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("Interactive transaction result: %v\n", result2)

	// ====== CHECK DATA AGAIN ======
	log.Println("All issues:")

	allIssues, err = issuesRepository.FindAll()
	if err != nil {
		log.Fatalf("Some error happened while find all: %v\n", err)
	}

	for _, issue := range allIssues {
		log.Printf("%v\n", issue)
	}

	// ====== CHECK AUTHOR INDEX ======
	log.Println("Find by index 'authorIndex':")

	author2Issues, err := issuesRepository.FindByAuthor("Author 2")
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("Author 2 issues: %v", author2Issues)

	// ====== CHECK TOPICS ======
	updateService, err := topic.NewStatusUpdateService(
		issuesRepository,
		queryHelper.Topic(),
	)
	if err != nil {
		log.Fatal(err)
	}

	log.Println("Update status for all tickets: NULL -> OPEN")
	for _, issue := range allIssues {
		var err = updateService.Update(ctx, issue.Id, "OPEN")
		if err != nil {
			log.Fatal(err)
		}
	}

	readerWorker, err := topic.NewReaderWorker(queryHelper.Topic())
	if err != nil {
		log.Fatal(err)
	}

	readerWorker.Run(ctx)

	log.Println("Update status for all tickets: NULL -> IN_PROGRESS")
	for _, issue := range allIssues {
		var err = updateService.Update(ctx, issue.Id, "IN_PROGRESS")
		if err != nil {
			log.Fatal(err)
		}
	}

	err = updateService.Shutdown(ctx)
	log.Println("Shutdown update service...")
	if err != nil {
		log.Fatal(err)
	}

	err = readerWorker.Shutdown(ctx)
	log.Println("Shutdown reader worker...")
	if err != nil {
		log.Fatal(err)
	}

	log.Println("Print all issues")

	allIssues, err = issuesRepository.FindAll()
	if err != nil {
		log.Fatal(err)
	}

	for _, issue := range allIssues {
		log.Printf("%v\n", issue)
	}

	// ====== CHANGEFEED TEST ======
	log.Println("Testing changefeed...")
	readerChangefeedWorker, err := topic.NewReaderChangefeedWorker(queryHelper.Topic())
	if err != nil {
		log.Fatal(err)
	}

	readerChangefeedWorker.ReadChangefeed(ctx)

	err = issuesRepository.UpdateStatus(first.Id, "FUTURE")
	if err != nil {
		log.Fatal(err)
	}

	err = issuesRepository.Delete(second.Id)
	if err != nil {
		log.Fatal(err)
	}

	err = issuesRepository.Delete(second.Id)
	if err != nil {
		log.Fatal(err)
	}

	readerChangefeedWorker.Shutdown(ctx)

	log.Println("Print all issues")

	allIssues, err = issuesRepository.FindAll()
	if err != nil {
		log.Fatal(err)
	}

	for _, issue := range allIssues {
		log.Printf("%v\n", issue)
	}

	// ====== COMPLEX QUERIES TEST ======
	log.Println("Testing complex queries...")

	err = issuesRepository.AddIssues([]string{
		"Ticket 4",
		"Ticket 5",
		"Ticket 6",
	})
	if err != nil {
		log.Fatal(err)
	}

	log.Println("Print all issues")

	allIssues, err = issuesRepository.FindAll()
	if err != nil {
		log.Fatal(err)
	}

	for _, issue := range allIssues {
		log.Printf("%v\n", issue)
	}

	log.Println("Update all issues' status")

	for _, issue := range allIssues {
		err = issuesRepository.UpdateStatus(issue.Id, "FUTURE")
		if err != nil {
			log.Fatal(err)
		}
	}

	log.Println("Find by ids:")

	foundByIds, err := issuesRepository.FindByIds([]uuid.UUID{
		third.Id,
		allIssues[4].Id,
	})
	if err != nil {
		log.Fatal(err)
	}

	for _, issue := range foundByIds {
		log.Printf("%v\n", issue)
	}

	log.Println("Future issues:")

	futureIssues, err := issuesRepository.FindFutures()
	if err != nil {
		log.Fatal(err)
	}

	for _, issue := range futureIssues {
		log.Printf("%v\n", issue)
	}

	log.Println("Delete issues by id")

	err = issuesRepository.DeleteByIds([]uuid.UUID{
		first.Id,
		allIssues[3].Id,
		secondIssue.Id,
	})
	if err != nil {
		log.Fatal(err)
	}

	log.Println("Print all issues")

	allIssues, err = issuesRepository.FindAll()
	if err != nil {
		log.Fatal(err)
	}

	for _, issue := range allIssues {
		log.Printf("%v\n", issue)
	}
}
