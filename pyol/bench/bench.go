package main

import (
	"bytes"
	"container/list"
	//"bytes"
	"encoding/json"
	"fmt"
	"github.com/urfave/cli"
	"io/ioutil"
	"net/http"
	"path/filepath"
	"strings"
	"time"

	//"log"
	Logger "github.com/withmandala/go-log"

	//"log"
	//"net/http"
	"os"

	//"strings"
	//"time"
)

var log = Logger.New(os.Stderr).WithColor().WithDebug().WithTimestamp()

type Call struct {
	name string
}

type Result struct {
	name     string
	start    time.Time
	end      time.Time
	duration float64
	err      error
}

func (r Result) CSVString() string {
	status := "Success"
	return fmt.Sprintf("%s,%s,%.3f,%.3f,%.3fs", r.name, status, r.start, r.end, r.duration)
}

func (r Result) String() string {
	status := "Success"
	return fmt.Sprintf("%s(%s, duration=%.3f)", r.name, status, r.duration)
}

func task(task int, urlbase string, reqQ chan Call, respQ chan *list.List, errQ chan error) {

	responses := list.New()

	for {
		call, hasNext := <-reqQ
		//log.Debugf("Receive call: %s\n", call)
		if !hasNext {
			errQ <- nil
			respQ <- responses
			return
		}
		// TODO: Better join of url strings
		url := fmt.Sprintf("%s/%s", urlbase, call.name)

		//log.Debugf("Request start: %s\n", call)
		start := time.Now()
		resp, err := http.Post(url, "text/json", bytes.NewBuffer([]byte("null")))
		end := time.Now()
		duration := end.Sub(start).Seconds()
		//log.Debugf("Request end: %s (duration=%v)\n", call, duration)

		if err != nil {
			errQ <- fmt.Errorf("Failed req to %s: %v", url, err)
			return
		}

		body, err := ioutil.ReadAll(resp.Body)
		resp.Body.Close()

		if err != nil {
			errQ <- fmt.Errorf("Failed req to %s, could not read body: %v", url, err)
			return
		}

		if resp.StatusCode != http.StatusOK {
			errQ <- fmt.Errorf("Failed req to %s: status %d, text '%s'", url, resp.StatusCode, string(body))
		}

		responses.PushBack(Result{
			name:     call.name,
			start:    start,
			end:      end,
			duration: duration,
		})

	}

}

//Run the workflow
func run(workload map[string]interface{}, tasks int, urlbase string, output_path string) error {

	calls := workload["calls"].([]interface{})

	reqQ := make(chan Call, 8)
	respQ := make(chan *list.List, tasks)
	errQ := make(chan error, tasks)
	for i := 0; i < tasks; i++ {
		go task(i, urlbase, reqQ, respQ, errQ)
	}
	log.Debug("Start workflow")

	responses := list.New()

	t0 := time.Now()
	for _, ucall := range calls {
		call := ucall.(map[string]interface{})
		select {
		case reqQ <- Call{name: call["name"].(string)}:
		case err := <-errQ:
			log.Fatal(err)
		}
	}
	close(reqQ)

	for i := 0; i < tasks; i++ {
		if err := <-errQ; err != nil {
			log.Fatal(err)
		}
	}
	t1 := time.Now()

	log.Debug("Finished sending requests\n")

	for i := 0; i < tasks; i++ {
		resp_list := <-respQ
		for e := resp_list.Front(); e != nil; e = e.Next() {
			log.Debugf("%v", e.Value)
			responses.PushBack(e.Value)
		}
	}
	// Dump all responses out

	seconds := t1.Sub(t0).Seconds()
	result := fmt.Sprintf("{\"seconds\": %.3f, \"ops/s\": %.3f}", seconds, float64(len(calls))/seconds)
	if output_path != "" {
		fmt.Printf(result)
	} else {
		data := []byte(result)
		if err := ioutil.WriteFile(output_path, data, 0644); err != nil {
			log.Fatal(err)
		}
	}

	return nil
}

//Register the scripts that use the workflow
func register(workload map[string]interface{}, path string) error {
	funcs := workload["funcs"].([]interface{})
	log.Debugf("Registry base path: %s\n", path)
	for _, _fn := range funcs {
		fn := _fn.(map[string]interface{})
		name := fn["name"].(string)
		path := filepath.Join(path, "registry", name)
		_lines := fn["code"].([]interface{})
		lines := []string{}
		for _, line := range _lines {
			lines = append(lines, line.(string))
		}
		// TODO: Check if the file has existed
		if _, err := os.Stat(path); !os.IsNotExist(err) {
			log.Debugf("Skip existed func %s: %s", name, path)
			continue
		}
		code := strings.Join(lines, "\n")
		log.Debugf("Register %s to %s\n", name, path)
		if err := ioutil.WriteFile(path, []byte(code), 0400); err != nil {
			log.Fatal(err)
		}
	}
	log.Debugf("Register all success\n")
	return nil
}

func getWorkload(workload_path string) map[string]interface{} {
	raw, err := ioutil.ReadFile(workload_path)
	if err != nil {
		log.Fatal(err.Error())
	}

	var workload map[string]interface{}
	if err := json.Unmarshal(raw, &workload); err != nil {
		log.Fatal(err.Error())
	}
	log.Debugf("Use workload: %s\n", workload_path)
	return workload
}

func main() {

	app := cli.NewApp()
	app.Name = "olbench"
	app.Usage = "Benchmark tool for Open-Lambda"
	app.UsageText = "olbench COMMAND [ARG...]"
	app.ArgsUsage = "ArgsUsage"
	app.EnableBashCompletion = true
	app.HideVersion = true

	logFlag := cli.BoolFlag{Name: "verbose", Aliases: []string{"v"},}
	taskFlag := cli.IntFlag{Name: "tasks, t", Value: 1, Aliases: []string{"t"},}
	portFlag := cli.StringFlag{Name: "port", Value: "5000", Aliases: []string{"p"},}
	workFlag := cli.StringFlag{Name: "workload", Value: "workload.json", Aliases: []string{"w"}}
	pathFlag := cli.StringFlag{
		Name: "path", Usage: "Path location for OL environment", Value: "default-ol", Aliases: []string{"p"},}
	outFlag := cli.StringFlag{Name: "output", Aliases: []string{"o"}}

	app.Commands = []*cli.Command{
		&cli.Command{
			Name:        "register",
			Usage:       "Register functions from a workload JSON file.",
			UsageText:   "olbench register [--work=workload-path.json] [--path=PATH] [--verbose]",
			Description: "Register functions from a workload JSON file.",
			Flags:       []cli.Flag{&pathFlag, &logFlag, &workFlag},
			Action: func(context *cli.Context) error {
				if !context.Bool("verbose") {
					log.WithoutDebug()
				}
				path := context.String("path")
				workload := getWorkload(context.String("workload"))
				return register(workload, path)
			},
		},

		&cli.Command{
			Name:        "run",
			Usage:       "Run workflow from a JSON workflow file.",
			UsageText:   "olbench run [--work=workload-path.json] [--port=5000] [--tasks=1] [--output=OUTPATH.json] [--verbose]",
			Description: "Register functions from a workload JSON file.",
			Flags:       []cli.Flag{&workFlag, &portFlag, &taskFlag, &logFlag, &outFlag},
			Action: func(context *cli.Context) error {
				if !context.Bool("verbose") {
					log.WithoutDebug()
				}
				tasks := context.Int("tasks")
				port := context.String("port")
				workload := getWorkload(context.String("workload"))
				//TODO: Change the host / urlbase if possible
				urlbase := fmt.Sprintf("http://localhost:%s/run", port)
				output := context.String("output")
				return run(workload, tasks, urlbase, output)
			},
		},
	}

	app.Setup()
	err := app.Run(os.Args)
	if err != nil {
		log.Fatal(err)
	}

}
