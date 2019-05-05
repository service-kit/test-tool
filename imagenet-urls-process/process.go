package imagenet_urls_process

import (
	"bufio"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"
)

const (
	URLS_FILE_NAME       = "./fall11_urls.txt"
	URLS_VALID_FILE_NAME = "./urls.txt"
	PROCESS_LIMIT        = 10000
	IMAGE_OUT_PATH       = "./image/"
)

func urlCheck(url string) (bool, io.ReadCloser) {
	c := http.Client{}
	rsp, err := c.Get(url)
	if nil != err {
		return false, nil
	}
	defer rsp.Body.Close()
	if http.StatusOK != rsp.StatusCode {
		return false, nil
	}
	ctp := rsp.Header.Get("Content-Type")
	height := rsp.Header.Get("Imageheight")
	width := rsp.Header.Get("Imagewidth")
	if strings.Contains(ctp, "image/jpeg") &&
		"" != height &&
		"" != width {
		return true, rsp.Body
	}
	return false, nil
}

func CheckUrls() error {
	f, err := os.OpenFile(URLS_FILE_NAME, os.O_RDONLY, 0777)
	if nil != err {
		return err
	}
	defer f.Close()

	af, err := os.OpenFile(URLS_VALID_FILE_NAME, os.O_WRONLY|os.O_TRUNC, 0777)
	if nil != err {
		return err
	}
	defer af.Close()

	br := bufio.NewReader(f)

	waitCheckUrlChan := make(chan string, PROCESS_LIMIT)
	waitWriteUrlChan := make(chan string, PROCESS_LIMIT)
	quitChan := make(chan byte, 1)

	processCheckChan := make([]chan string, PROCESS_LIMIT)

	wgCheck := new(sync.WaitGroup)

	processCheck := func(input chan string) {
		wgCheck.Add(1)
		go func() {
			defer wgCheck.Done()
			for {
				url := <-input
				if "" == url {
					break
				}
				fmt.Println("recv url", url)
				if isValid, reader := urlCheck(url); isValid {
					waitWriteUrlChan <- url
					f, err := os.OpenFile(IMAGE_OUT_PATH+url, os.O_TRUNC|os.O_WRONLY, 0777)
					if nil != err {
						break
					}
					defer f.Close()
					defer reader.Close()
					io.Copy(f, reader)
				}
				time.Sleep(10 * time.Millisecond)
			}
		}()
	}

	for i := 0; i < PROCESS_LIMIT; i++ {
		curChan := make(chan string, 1024)
		processCheckChan[i] = curChan
		processCheck(curChan)
	}

	// porcess check imput
	go func() {
		chanIdx := 0
		for {
			url := <-waitCheckUrlChan
			if "" == url {
				fmt.Println("need check chan closed")
				break
			}
			processCheckChan[chanIdx] <- url
			chanIdx++
			chanIdx %= PROCESS_LIMIT
			continue
		}
		for i := 0; i < PROCESS_LIMIT; i++ {
			processCheck(processCheckChan[i])
		}
	}()

	// porcess write to file
	go func() {
		for {
			url := <-waitWriteUrlChan
			fmt.Println("recv write url", url)
			if "" == url {
				fmt.Println("need write chan closed")
				close(quitChan)
				return
			}
			af.WriteString(url)
			af.Write([]byte("\r\n"))
		}
	}()

	go func() {
		for {
			data, _, c := br.ReadLine()
			if c == io.EOF {
				break
			}
			url := string(data)
			strs := strings.Split(url, "\t")
			if len(strs) != 2 {
				fmt.Println("url data err", url)
				continue
			}
			waitCheckUrlChan <- strs[1]
		}
		close(waitCheckUrlChan)
	}()

	wgCheck.Wait()
	select {
	case <-quitChan:
		break
	}
	return nil
}
