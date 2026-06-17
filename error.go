package gopipe

import "fmt"

type StepError struct {
	StepName string
	Err      error
}

func (e *StepError) Error() string {
	return fmt.Sprintf("%s: %s", e.StepName, e.Err.Error())
}

func (e *StepError) Unwrap() error {
	return e.Err
}
