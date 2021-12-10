package txn

const errCodeInterrupted byte = 1
const errCodeDeadline byte = 2
const errCodeClosed byte = 3

type txnErr interface {
	error
	errorCode() byte
}

type InterruptedError struct{}

func (InterruptedError) Error() string {
	return "interrupted"
}

func (InterruptedError) errorCode() byte {
	return errCodeInterrupted
}

type DeadlineError struct{}

func (DeadlineError) Error() string {
	return "deadline"
}

func (DeadlineError) errorCode() byte {
	return errCodeDeadline
}

type ClosedError struct{}

func (ClosedError) Error() string {
	return "closed"
}

func (ClosedError) errorCode() byte {
	return errCodeClosed
}
