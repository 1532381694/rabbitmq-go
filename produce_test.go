package rabbitmq

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"
)

func TestProduce(t *testing.T) {
	Produce()

	for i := 1; i < 3; i++ {

		message, _ := json.Marshal(Info{Message: "zhansan", ParentId: "00000", Money: i})
		go SendMessage(message, LOGIN, 0)
		message2, _ := json.Marshal(Info{Message: "zhansan", ParentId: "00000", Money: i})
		go SendMessage(message2, LOGIN, 0)

	}
	select {
	case <-time.After(1000 * time.Second):

	}
}
