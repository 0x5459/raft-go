package main

import (
	"0x5459/raft-go"
	"encoding/base64"
	"errors"
	"fmt"
	"github.com/imroc/req/v3"
	"github.com/urfave/cli/v2"
	"log"
	"os"
	"time"
)

type RaftError struct {
	ErrNo int    `json:"err_no"`
	Err   string `json:"error"`
	Data  string `json:"leader_url"`
}

func main() {
	httpClient := req.C()
	var serverUrl string
	err := (&cli.App{
		Name:    "raft-go-example-client",
		Version: "0.1",
		Flags: []cli.Flag{&cli.StringFlag{
			Name:        "url",
			Usage:       "server url",
			Destination: &serverUrl,
		}},
		Commands: []*cli.Command{
			{
				Name:    "RM",
				Aliases: []string{"rm"},
				Usage:   "RM <key>",
				Action: func(c *cli.Context) error {
					if c.NArg() < 1 {
						return errors.New("key cannot be empty")
					}
					key := c.Args().Get(0)
					printErrAndLatency(sendRmReq(httpClient, serverUrl, key))
					return nil
				},
			},
			{
				Name:    "SET",
				Aliases: []string{"set"},
				Usage:   "SET <key> <value>",
				Action: func(c *cli.Context) error {
					if c.NArg() < 1 {
						return errors.New("key cannot be empty")
					} else if c.NArg() < 2 {
						return errors.New("value cannot be empty")
					}
					key := c.Args().Get(0)
					value := c.Args().Get(1)
					printErrAndLatency(sendSetReq(httpClient, serverUrl, key, value))
					return nil
				},
			},
			{
				Name:    "GET",
				Aliases: []string{"get"},
				Usage:   "GET key",
				Action: func(c *cli.Context) error {
					if c.NArg() < 1 {
						return errors.New("key cannot be empty")
					}
					key := c.Args().Get(0)
					var res struct {
						Exist bool   `json:"exist"`
						Value string `json:"value"`
					}
					r := httpClient.R().
						SetQueryParam("key", key).
						SetResult(&res)

					latency, err := sendReq(r, serverUrl, func(r *req.Request, serverUrl string) (*req.Response, error) {
						return r.Get(serverUrl + "/get_key")
					})
					if err != nil {
						printError(err)
					} else {
						if res.Exist {
							fmt.Println(res.Value)
						} else {
							fmt.Println("<nil>")
						}
					}
					printLatency(latency)
					return nil
				},
			},
		},
	}).Run(os.Args)

	if err != nil {
		log.Fatal(err)
	}
}

func printErrAndLatency(latency time.Duration, err error) {
	if err != nil {
		printError(err)
	}
	printLatency(latency)
}
func printError(err error) {
	_, _ = fmt.Fprintf(os.Stderr, "error: %s\n", err.Error())
}

func printLatency(latency time.Duration) {
	fmt.Println()
	fmt.Println("--------------------")
	fmt.Printf("latency: <%.2f sec>\n", float64(latency)/float64(time.Second))
}

func sendRmReq(httpClient *req.Client, serverUrl, key string) (time.Duration, error) {
	return sendAppendReq(httpClient, serverUrl, &raft.MsmCommand{
		Op:  raft.MsmRm,
		Key: key,
	})
}

func sendSetReq(httpClient *req.Client, serverUrl, key string, value string) (time.Duration, error) {
	return sendAppendReq(httpClient, serverUrl, &raft.MsmCommand{
		Op:    raft.MsmSet,
		Key:   key,
		Value: value,
	})
}

func sendAppendReq(httpClient *req.Client, serverUrl string, cmd *raft.MsmCommand) (time.Duration, error) {
	cmdBytes, err := cmd.Marshal()
	if err != nil {
		return 0, fmt.Errorf("failed to marshel command. err: %s", err)
	}
	r := httpClient.R().SetBodyJsonMarshal(map[string]string{
		"command": base64.URLEncoding.EncodeToString(cmdBytes),
	})
	return sendReq(r, serverUrl, func(r *req.Request, serverUrl string) (*req.Response, error) {
		return r.Post(serverUrl + "/append")
	})
}

func sendReq(request *req.Request, serverUrl string, doReq func(request *req.Request, serverUrl string) (*req.Response, error)) (time.Duration, error) {
	var raftErr RaftError
	request.SetError(&raftErr)
	start := time.Now()
	resp, err := doReq(request, serverUrl)
	if err != nil {
		return 0, fmt.Errorf("failed to send http request. err: %s", err)
	}
	latency := time.Since(start)
	if !resp.IsSuccess() {
		if raftErr.ErrNo == 1 {
			// not leader
			return sendReq(request, raftErr.Data, doReq)
		}
		return latency, errors.New(raftErr.Err)
	}
	return latency, nil
}
