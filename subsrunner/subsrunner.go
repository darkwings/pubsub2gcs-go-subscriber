package main

import (
	"os"

	"frank.com/gcp/subscriber"
)

/*
  export GOOGLE_APPLICATION_CREDENTIALS=/Users/ETORRIFUT/work/SA.json

  Da eseguire con ./subsrunner training-gcp-309207 topic-frank-sub-ordered
*/
func main() {

	projectId := os.Args[1] // training-gcp-309207
	topicSub := os.Args[2]  // topic-frank-sub

	subscriber.PullMsgsConcurrencyControl(os.Stdout, projectId, topicSub)
}
