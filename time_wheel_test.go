package timewheel

import (
	"testing"
	"time"
)

func Test_timeWheel(t *testing.T){
	timewheel := NewTimeWheel(10, 500 * time.Millisecond)
	defer timewheel.Stop()

	timewheel.AddTask("test1", func() {
		t.Logf("test1, %v", time.Now())
	}, time.Now().Add(time.Second))

	timewheel.AddTask("test2", func() {
		t.Logf("test2, %v", time.Now())
	}, time.Now().Add(2 * time.Second))

	timewheel.AddTask("test2", func() {
		t.Logf("test2, %v", time.Now())
	}, time.Now().Add(5 * time.Second))

	<-time.After(6 * time.Second)
}
