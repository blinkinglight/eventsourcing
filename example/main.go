package main

import (
	"fmt"
	"time"

	"github.com/hallgren/eventsourcing"

	dsql "database/sql"

	"github.com/hallgren/eventsourcing/eventstore/sql"

	_ "github.com/mattn/go-sqlite3"
)

func main() {
	var c = make(chan eventsourcing.Event)
	// Setup a memory based event store
	sdb, _ := dsql.Open("sqlite3", "es.sqlite3")
	db := sql.Open(sdb)
	db.Migrate()
	repo := eventsourcing.NewRepository(db)
	repo.Register(&FrequentFlierAccountAggregate{})
	repo.Register(&Custom{})

	f := func(e eventsourcing.Event) {
		fmt.Printf("Event from stream %q\n", e)
		// Its a good practice making this function as fast as possible not blocking the event sourcing call for to long
		// Here we use a channel to store the events to be consumed async
		c <- e
	}
	sub := repo.Subscribers().All(f)
	defer sub.Close()

	// Read the event stream async
	go func() {
		for {
			// advance to next value
			event := <-c
			fmt.Println("STREAM EVENT")
			fmt.Println(event)
		}
	}()

	// Creates the aggregate and adds a second event
	aggregate := CreateFrequentFlierAccount("morgan")
	if err := repo.Get("for-test", aggregate); err != nil {
		aggregate.TrackChange(aggregate, "flight.a", &FrequentFlierAccountCreated{OpeningMiles: 0, OpeningTierPoints: 0})
	}

	err := repo.Save(aggregate)
	if err != nil {
		panic("Could not save the aggregate" + err.Error())
	}
	aggregate.RecordFlightTaken(10, 5)

	// saves the events to the memory backed eventstore
	err = repo.Save(aggregate)
	if err != nil {
		// repo.Get("for-test", aggregate)
		panic("Could not save the aggregate" + err.Error())
	}

	// Load the saved aggregate
	copy := FrequentFlierAccountAggregate{}
	err = repo.Get(string(aggregate.ID()), &copy)
	if err != nil {
		panic("Could not get aggregate")
	}

	// Sleep to make sure the events are delivered from the stream
	time.Sleep(time.Millisecond * 100)
	fmt.Println("AGGREGATE")
	fmt.Println(copy)

	var cm Custom
	err = repo.Get("for-test", &cm)
	if err != nil {
		panic(err)
	}
	cm.Print()

}
