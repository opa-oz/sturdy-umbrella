package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"strings"
)

type Request struct {
	Url     string  `json:"url"`
	Method  string  `json:"method,omitempty"`
	Headers Headers `json:"headers,omitempty"`
}

type Headers struct {
	Authorization string `json:"Authorization,omitempty"`
	ContentType   string `json:"Content-Type,omitempty"`
}

var (
	inputPath         *string
	commonHeadersPath *string
	outputPath        *string
)

func init() {
	inputPath = flag.String("requests", "", "JSON requests file")
	commonHeadersPath = flag.String("headers", "", "Common headers file")
	outputPath = flag.String("output", "", "Output file")
}

func makeRequest(request Request, headers Headers) []byte {
	var httpMethod string

	if len(request.Method) > 0 {
		httpMethod = request.Method
	} else {
		httpMethod = http.MethodGet
	}

	req, err := http.NewRequest(httpMethod, request.Url, nil)
	if err != nil {
		fmt.Printf("client: could not create request: %s\n", err)
		os.Exit(1)
	}

	if len(headers.ContentType) > 0 {
		req.Header.Set("Content-Type", headers.ContentType)
	}
	if len(headers.Authorization) > 0 {
		req.Header.Set("Authorization", headers.Authorization)
	}

	res, derr := http.DefaultClient.Do(req)
	if derr != nil {
		fmt.Printf("client: error making http request: %s\n", derr)
		os.Exit(1)
	}

	resBody, rerr := ioutil.ReadAll(res.Body)
	if rerr != nil {
		fmt.Printf("client: could not read response body: %s\n", rerr)
		os.Exit(1)
	}

	return resBody
}

func main() {
	flag.Parse()

	if len(*inputPath) == 0 {
		panic("There is no input file")
	}

	if len(*outputPath) == 0 {
		panic("There is no output file")
	}

	content, err := ioutil.ReadFile(*inputPath)
	if err != nil {
		fmt.Printf("error reading input file: %s\n", err)
		os.Exit(1)
	}

	var requests []Request
	err = json.Unmarshal(content, &requests)
	if err != nil {
		fmt.Printf("error unwrapping requests json: %s\n", err)
		os.Exit(1)
	}

	var headers Headers

	if len(*commonHeadersPath) != 0 {
		headersContent, herr := ioutil.ReadFile(*commonHeadersPath)
		if herr != nil {
			fmt.Printf("error reading headers file: %s\n", herr)
			os.Exit(1)
		}

		herr = json.Unmarshal(headersContent, &headers)
		if herr != nil {
			fmt.Printf("error unwrapping headers json: %s\n", herr)
			os.Exit(1)

		}
	}

	results := make([]string, len(requests))

	for index := range requests {
		request := requests[index]

		result := makeRequest(request, headers)
		results[index] = string(result)
	}

	_ = ioutil.WriteFile(*outputPath, []byte(fmt.Sprintf("[%s]", strings.Join(results[:], ","))), 0644)
}
