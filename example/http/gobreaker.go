package main

import (
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"time"

	"github.com/sony/gobreaker"
)

var cb *gobreaker.CircuitBreaker

func init() {
	var st gobreaker.Settings
	st.Name = "HTTP GET"
	st.ReadyToTrip = func(counts gobreaker.Counts) bool {
		failureRatio := float64(counts.TotalFailures) / float64(counts.Requests)
		return counts.Requests >= 3 && failureRatio >= 0.5
	}
	st.Timeout = time.Second * 10	// 从开启切换到半开的时间
	st.OnStateChange = func(name string, from, to gobreaker.State) {
		log.Printf("state change: [%v] -> [%v]\n", from, to)
	}
	cb = gobreaker.NewCircuitBreaker(st)
}

func Get(url string) ([]byte, error) {
	body, err := cb.Execute(func() (interface{}, error) {
		resp, err := http.Get(url)
		if err != nil {
			return nil, err
		}

		defer resp.Body.Close()
		if resp.StatusCode >= 400 {
			return nil, fmt.Errorf("[%v]%v", resp.StatusCode, http.StatusText(resp.StatusCode))
		}
		body, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return nil, err
		}

		return body, nil
	})

	if err != nil {
		return nil, err
	}
	return body.([]byte), nil
}

func main() {
	for {
		body, err := Get("http://localhost:9000")
		if err != nil {
			log.Println(err)
		}
		log.Printf("%v, %+v\n", cb.State(), cb.Counts())
		fmt.Println(string(body))
		time.Sleep(time.Millisecond * 500)
	}
}
