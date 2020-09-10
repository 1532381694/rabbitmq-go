package rabbitmq

import (
	"testing"
)

func TestClient(t *testing.T) {
	ConsumerStart()
	select {}
}
