package main

import (
	"fmt"
	"log"
	"os"
	"strings"
	"time"
	"todos/todos"

	"github.com/urfave/cli/v2"
)

func main() {

	err := todos.CreateMainBucketIfNotExists("todos")
	if err != nil {
		log.Fatal("create domain main bucket error: ", err)
	}

	err = todos.CreateUserIfNotExists("todos", "todo_user", "password")
	if err != nil {
		log.Fatal("create domain user error: ", err)
	}

	err = todos.CreatePrimaryIndex("todos")
	if err != nil {
		log.Fatal("create domain primary index error: ", err)
	}

	err = todos.AddDomain(&todos.AddDomainOptions{
		ConnStr:  "couchbase://127.0.0.1",
		Name:     "todos",
		Username: "todo_user",
		Password: "password",
	})
	if err != nil {
		log.Fatal("add domain error: ", err)
	}

	app := &cli.App{
		Name:  "todo",
		Usage: "todo [options]",
		Commands: []*cli.Command{
			{
				Name:    "list",
				Aliases: []string{"l"},
				Usage:   "list tasks",
				Flags: []cli.Flag{
					&cli.BoolFlag{
						Name:  "completed",
						Value: false,
						Usage: "show only completed items",
					},
					&cli.BoolFlag{
						Name:  "incompleted",
						Value: false,
						Usage: "show only incompleted items",
					},
				},
				Action: func(c *cli.Context) error {

					domainMgr, err := todos.GetDomain("todos")
					if err != nil {
						return err
					}

					onlyCompleted := false
					if c.Bool("completed") {
						onlyCompleted = true
					}

					onlyInCompleted := false
					if c.Bool("incompleted") {
						onlyInCompleted = true
					}

					query := "select meta().id ID, todos.* from todos"

					where := []string{}
					if onlyCompleted {
						where = append(where, "IsComplete = true")
					}

					if onlyInCompleted {
						where = append(where, "IsComplete = false")
					}

					if len(where) > 0 {
						query += " where " + strings.Join(where, " AND ")
					}

					rows, err := domainMgr.Query(query, &todos.QueryOptions{})
					if err != nil {
						return err
					}

					for rows.Next() {
						var t todos.Todo
						if err := rows.Row(&t); err != nil {
							return err
						}
						fmt.Printf("%+v\n", t)
					}

					return nil
				},
			},
			{
				Name:    "add",
				Aliases: []string{"a"},
				Usage:   "add a new task",
				Action: func(c *cli.Context) error {
					domainMgr, err := todos.GetDomain("todos")
					if err != nil {
						return err
					}

					id, err := domainMgr.Create(&todos.Todo{
						Text: c.Args().First(),
					})
					if err != nil {
						return err
					}

					fmt.Println("create todo with id: ", id)

					// no query consistency
					query := "select meta().id as ID, todos.* from todos where meta().id = ?"
					rows, err := domainMgr.Query(query, &todos.QueryOptions{}, id)
					if err != nil {
						return err
					}

					fmt.Println("check with not_bounded consistency level")
					var found bool
					for rows.Next() {
						var t todos.Todo
						if err := rows.Row(&t); err != nil {
							return err
						}
						found = true
						fmt.Printf("%+v\n", t)
					}

					if !found {
						fmt.Println("new item not found with not_bounded consistency level")
					}

					// with at_plus on
					// example for tracking mutation state
					query = "select meta().id as ID, todos.* from todos where meta().id = ?"
					rows, err = domainMgr.Query(query, &todos.QueryOptions{
						AtPlus:      true,
						Collections: []string{"todos"},
					}, []interface{}{id}...)
					if err != nil {
						return fmt.Errorf("todo after add query error: %w", err)
					}

					found = false
					for rows.Next() {
						var t todos.Todo
						if err := rows.Row(&t); err != nil {
							return err
						}
						found = true
						fmt.Printf("%+v\n", t)
					}

					if !found {
						log.Fatal("new item not found with at_plus mode")
					}

					return nil
				},
			},
			{
				Name:    "edit",
				Aliases: []string{"e"},
				Usage:   "edit an existed task",
				Action: func(c *cli.Context) error {

					domainMgr, err := todos.GetDomain("todos")
					if err != nil {
						return err
					}

					idArg := c.Args().First()
					text := c.Args().Get(1)

					var todo todos.Todo
					cas, err := domainMgr.Get(idArg, &todo)
					if err != nil {
						return err
					}

					todo.Text = text

					_, err = domainMgr.Update(idArg, cas, &todo)
					if err != nil {
						return err
					}

					fmt.Println("todo updated")

					return nil
				},
			},
			{
				Name:    "complete",
				Aliases: []string{"c"},
				Usage:   "complete a task on the list",
				Action: func(c *cli.Context) error {

					domainMgr, err := todos.GetDomain("todos")
					if err != nil {
						return err
					}

					idArg := c.Args().First()

					var todo todos.Todo
					cas, err := domainMgr.Get(idArg, &todo)
					if err != nil {
						return err
					}

					todo.IsComplete = true
					todo.Datetime = time.Now().UTC()

					_, err = domainMgr.Update(idArg, cas, &todo)
					if err != nil {
						return err
					}

					fmt.Println("todo completed")

					return nil
				},
			},
			{
				Name:    "remove",
				Aliases: []string{"r"},
				Usage:   "remove the task from the list",
				Action: func(c *cli.Context) error {
					domainMgr, err := todos.GetDomain("todos")
					if err != nil {
						return err
					}

					idArg := c.Args().First()
					var todo todos.Todo

					cas, err := domainMgr.Get(idArg, &todo)
					if err != nil {
						return err
					}

					if _, err := domainMgr.Delete(idArg, cas); err != nil {
						return err
					}

					fmt.Println("todo removed")

					return nil
				},
			},
		},
	}

	err = app.Run(os.Args)
	if err != nil {
		log.Fatal(err)
	}
}
