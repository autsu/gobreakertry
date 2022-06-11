// Package gobreaker implements the Circuit Breaker pattern.
// See https://msdn.microsoft.com/en-us/library/dn589784.aspx.
package gobreaker

import (
	"errors"
	"fmt"
	"sync"
	"time"
)

// State is a type that represents a state of CircuitBreaker.
type State int

// These constants are states of CircuitBreaker.
// 熔断器的状态
const (
	// StateClosed 表示关闭状态，此时所有请求都会通过
	StateClosed State = iota
	// StateHalfOpen 半开状态，会根据情况变更为开启或者关闭状态
	StateHalfOpen
	// StateOpen 开启状态，此时会拒绝所有请求
	StateOpen
)

var (
	// ErrTooManyRequests is returned when the CB state is half open and the requests count is over the cb maxRequests
	// 该错误在状态为半开且请求数超过 maxRequests 时返回
	ErrTooManyRequests = errors.New("too many requests")
	// ErrOpenState is returned when the CB state is open
	// 该错误在状态为开启时返回
	ErrOpenState = errors.New("circuit breaker is open")
)

// String implements stringer interface.
// String 继承了 stringer 接口，相当于自定义了 fmt.Println(State) 的输出
func (s State) String() string {
	switch s {
	case StateClosed:
		return "closed"
	case StateHalfOpen:
		return "half-open"
	case StateOpen:
		return "open"
	default:
		return fmt.Sprintf("unknown state: %d", s)
	}
}

// Counts holds the numbers of requests and their successes/failures.
// CircuitBreaker clears the internal Counts either
// on the change of the state or at the closed-state intervals.
// Counts ignores the results of the requests sent before clearing.
// Counts 保存请求的数量及其成功失败的次数。
// CircuitBreaker 在状态更改或关闭状态间隔时清除内部计数。
// Counts 会忽略在清除之前发送的请求的结果。
type Counts struct {
	Requests             uint32 // 总请求次数
	TotalSuccesses       uint32 // 总成功次数
	TotalFailures        uint32 // 总失败次数
	ConsecutiveSuccesses uint32 // 连续成功次数
	ConsecutiveFailures  uint32 // 连续失败次数
}

func (c *Counts) onRequest() {
	c.Requests++
}

func (c *Counts) onSuccess() {
	c.TotalSuccesses++
	c.ConsecutiveSuccesses++
	c.ConsecutiveFailures = 0
}

func (c *Counts) onFailure() {
	c.TotalFailures++
	c.ConsecutiveFailures++
	c.ConsecutiveSuccesses = 0
}

func (c *Counts) clear() {
	c.Requests = 0
	c.TotalSuccesses = 0
	c.TotalFailures = 0
	c.ConsecutiveSuccesses = 0
	c.ConsecutiveFailures = 0
}

// Settings configures CircuitBreaker:
//
// Name is the name of the CircuitBreaker.
//
// MaxRequests is the maximum number of requests allowed to pass through
// when the CircuitBreaker is half-open.
// If MaxRequests is 0, the CircuitBreaker allows only 1 request.
//
// Interval is the cyclic period of the closed state
// for the CircuitBreaker to clear the internal Counts.
// If Interval is less than or equal to 0, the CircuitBreaker doesn't clear internal Counts during the closed state.
//
// Timeout is the period of the open state,
// after which the state of the CircuitBreaker becomes half-open.
// If Timeout is less than or equal to 0, the timeout value of the CircuitBreaker is set to 60 seconds.
//
// ReadyToTrip is called with a copy of Counts whenever a request fails in the closed state.
// If ReadyToTrip returns true, the CircuitBreaker will be placed into the open state.
// If ReadyToTrip is nil, default ReadyToTrip is used.
// Default ReadyToTrip returns true when the number of consecutive failures is more than 5.
//
// OnStateChange is called whenever the state of the CircuitBreaker changes.
//
// IsSuccessful is called with the error returned from a request.
// If IsSuccessful returns true, the error is counted as a success.
// Otherwise the error is counted as a failure.
// If IsSuccessful is nil, default IsSuccessful is used, which returns false for all non-nil errors.
type Settings struct {
	// 熔断器的名称
	Name string

	// MaxRequests 是 CircuitBreaker 半开时允许通过的最大请求数。
	// 如果 MaxRequests 为 0，则 CircuitBreaker 只允许 1 个请求。
	// FIXME 比较迷的一个变量，源码里有两种情况：
	// 1. 请求总数（Requests） >= MaxRequests，那么会返回请求过多的错误
	// 2. 连续成功次数（ConsecutiveSuccesses） >= MaxRequests，那么变更为关闭状态
	// 1 在 beforeRequest() 函数中，2 在 afterRequest() -> onSuccess() 的 HalfOpen 分支中
	// 也就是在请求前会确保请求总数不超过 MaxRequest，请求后如果处于半开状态且连续成功数 >= MaxRequest
	// 那么会变更为关闭状态
	MaxRequests uint32

	// Interval 是熔断器处于关闭状态时，定期清除内部 Counts 的时间。
	// 如果 Interval 小于或等于 0，CircuitBreaker 在关闭状态期间不会清除内部计数。
	// FIXME 这个东西暂时没发现用处何在
	// 在网上找了一个分析，意思是如果一直处于成功状态，那么计数的意义就不是很大，
	// 需要定期清空，不然可能会溢出
	Interval time.Duration

	// Timeout 是打开状态的持续时间，到时后会变更为半打开状态。
	// 如果 Timeout 小于或等于 0，则将 CircuitBreaker 的超时值设置为 60 秒。
	Timeout time.Duration

	// 每当请求在关闭状态下失败时，就会调用 ReadyToTrip，参数传递的是 Counts 的副本。
	// 如果 ReadyToTrip 返回 true，CircuitBreaker 将进入打开状态。
	// 如果 ReadyToTrip 为 nil，则使用默认 ReadyToTrip。
	// 当连续失败次数超过 5 次时，默认 ReadyToTrip 返回 true。
	ReadyToTrip func(counts Counts) bool

	// OnStateChange 是熔断器状态变更时的回调函数
	OnStateChange func(name string, from State, to State)

	// IsSuccessful 判断请求是否成功，传入的 err 是执行用户请求函数后返回的。
	// （也就是 CircuitBreaker.Execute 的参数 req）
	// 如果 IsSuccessful 返回 true， 则说明请求发生了错误，否则说明没有错误。
	// 如果 IsSuccessful 为 nil， 则使用默认 IsSuccessful，该默认函数的逻辑是：
	// if err == nil { return true }
	IsSuccessful func(err error) bool
}

// CircuitBreaker is a state machine to prevent sending requests that are likely to fail.
type CircuitBreaker struct {
	// 虚线内的属性和 Settings 中的相同，如果 Settings 中没有设置，则使用默认值来填充
	// ==================
	name string
	// 比较迷的一个变量，源码里有两种情况：
	// 1. 请求总数（Requests） >= MaxRequests，那么会返回请求过多的错误
	// 2. 连续成功次数（ConsecutiveSuccesses） >= MaxRequests，那么变更为关闭状态
	// 1 在 beforeRequest() 函数中，2 在 afterRequest() -> onSuccess() 的 HalfOpen 分支中
	// 也就是在请求前会确保请求总数不超过 MaxRequest，请求后如果处于半开状态且连续成功数 >= MaxRequest
	// 那么会变更为关闭状态
	maxRequests uint32

	// 关闭状态下定期清空计数的时间，如果为 0，则不清空
	// 这里我不太明白清空计数的原因，在网上找了一个分析，意思是如果一直处于成功状态，
	// 那么计数的意义就不是很大，此外如果请求量过大可能会导致溢出，所以需要定期清空
	interval time.Duration

	// 打开状态的持续时间，到时后会变更为半打开状态。
	timeout time.Duration

	// 关闭状态下会调用该回调函数，如果返回 true，则进入打开状态
	readyToTrip func(counts Counts) bool

	// 用来判断请求是否成功的回调函数
	isSuccessful func(err error) bool

	// 发生状态变更时的回调函数
	onStateChange func(name string, from State, to State)
	// ====================

	mutex      sync.Mutex
	state      State
	generation uint64
	counts     Counts
	// 这个变量貌似有两种情况：
	// 1. 开启状态下，代表切换到半开启的绝对时间（time.Time 代表一个绝对时间）
	//    具体值是 time.Now + timeout
	// 2. 关闭状态下，代表进入下一个周期的绝对时间（进入下一个周期会清空计数）
	// 	  具体值是 time.Now + interval
	expiry time.Time
}

// TwoStepCircuitBreaker is like CircuitBreaker but instead of surrounding a function
// with the breaker functionality, it only checks whether a request can proceed and
// expects the caller to report the outcome in a separate step using a callback.
type TwoStepCircuitBreaker struct {
	cb *CircuitBreaker
}

// NewCircuitBreaker returns a new CircuitBreaker configured with the given Settings.
func NewCircuitBreaker(st Settings) *CircuitBreaker {
	cb := new(CircuitBreaker)

	cb.name = st.Name
	cb.onStateChange = st.OnStateChange

	if st.MaxRequests == 0 {
		cb.maxRequests = 1
	} else {
		cb.maxRequests = st.MaxRequests
	}

	if st.Interval <= 0 {
		cb.interval = defaultInterval
	} else {
		cb.interval = st.Interval
	}

	if st.Timeout <= 0 {
		cb.timeout = defaultTimeout
	} else {
		cb.timeout = st.Timeout
	}

	if st.ReadyToTrip == nil {
		cb.readyToTrip = defaultReadyToTrip
	} else {
		cb.readyToTrip = st.ReadyToTrip
	}

	if st.IsSuccessful == nil {
		cb.isSuccessful = defaultIsSuccessful
	} else {
		cb.isSuccessful = st.IsSuccessful
	}

	cb.toNewGeneration(time.Now())

	return cb
}

// NewTwoStepCircuitBreaker returns a new TwoStepCircuitBreaker configured with the given Settings.
func NewTwoStepCircuitBreaker(st Settings) *TwoStepCircuitBreaker {
	return &TwoStepCircuitBreaker{
		cb: NewCircuitBreaker(st),
	}
}

const defaultInterval = time.Duration(0) * time.Second
const defaultTimeout = time.Duration(60) * time.Second

func defaultReadyToTrip(counts Counts) bool {
	return counts.ConsecutiveFailures > 5
}

func defaultIsSuccessful(err error) bool {
	return err == nil
}

// Name returns the name of the CircuitBreaker.
func (cb *CircuitBreaker) Name() string {
	return cb.name
}

// State returns the current state of the CircuitBreaker.
func (cb *CircuitBreaker) State() State {
	cb.mutex.Lock()
	defer cb.mutex.Unlock()

	now := time.Now()
	state, _ := cb.currentState(now)
	return state
}

// Counts returns internal counters
func (cb *CircuitBreaker) Counts() Counts {
	cb.mutex.Lock()
	defer cb.mutex.Unlock()

	return cb.counts
}

// Execute runs the given request if the CircuitBreaker accepts it.
// Execute returns an error instantly if the CircuitBreaker rejects the request.
// Otherwise, Execute returns the result of the request.
// If a panic occurs in the request, the CircuitBreaker handles it as an error
// and causes the same panic again.
func (cb *CircuitBreaker) Execute(req func() (interface{}, error)) (interface{}, error) {
	// 执行请求前
	generation, err := cb.beforeRequest()
	if err != nil {
		return nil, err
	}

	defer func() {
		e := recover()
		if e != nil {
			cb.afterRequest(generation, false)
			panic(e)
		}
	}()

	result, err := req()
	// 执行请求后
	cb.afterRequest(generation, cb.isSuccessful(err))
	return result, err
}

// Name returns the name of the TwoStepCircuitBreaker.
func (tscb *TwoStepCircuitBreaker) Name() string {
	return tscb.cb.Name()
}

// State returns the current state of the TwoStepCircuitBreaker.
func (tscb *TwoStepCircuitBreaker) State() State {
	return tscb.cb.State()
}

// Counts returns internal counters
func (tscb *TwoStepCircuitBreaker) Counts() Counts {
	return tscb.cb.Counts()
}

// Allow checks if a new request can proceed. It returns a callback that should be used to
// register the success or failure in a separate step. If the circuit breaker doesn't allow
// requests, it returns an error.
func (tscb *TwoStepCircuitBreaker) Allow() (done func(success bool), err error) {
	generation, err := tscb.cb.beforeRequest()
	if err != nil {
		return nil, err
	}

	return func(success bool) {
		tscb.cb.afterRequest(generation, success)
	}, nil
}

func (cb *CircuitBreaker) beforeRequest() (uint64, error) {
	cb.mutex.Lock()
	defer cb.mutex.Unlock()

	now := time.Now()
	state, generation := cb.currentState(now)

	// 如果熔断器处于开启状态，直接返回错误，因为该方法在 Execute 中先于用户请求执行，
	// 且逻辑是有 err 直接 return，所以不会执行之后的代码，具体查看 Execute，下面是截取的部分：
	// generation, err := cb.beforeRequest()
	//	if err != nil {
	//		return nil, err
	//	}
	if state == StateOpen {
		return generation, ErrOpenState
		// 请求前如果处于半开状态，会进行限流操作
	} else if state == StateHalfOpen && cb.counts.Requests >= cb.maxRequests {
		return generation, ErrTooManyRequests
	}

	cb.counts.onRequest() // 更新计数
	return generation, nil
}

func (cb *CircuitBreaker) afterRequest(before uint64, success bool) {
	cb.mutex.Lock()
	defer cb.mutex.Unlock()

	now := time.Now()
	state, generation := cb.currentState(now)
	if generation != before {
		return
	}

	// 更新状态和计数
	if success {
		cb.onSuccess(state, now)
	} else {
		cb.onFailure(state, now)
	}
}

// 熔断器请求成功时调用该函数
func (cb *CircuitBreaker) onSuccess(state State, now time.Time) {
	switch state {
	case StateClosed: // 如果此时是关闭状态，则更新计数
		cb.counts.onSuccess()
	case StateHalfOpen: // 半开状态
		cb.counts.onSuccess() // 更新计数
		// 连续成功总数超过了设置的 maxRequests，变更为关闭状态
		if cb.counts.ConsecutiveSuccesses >= cb.maxRequests {
			cb.setState(StateClosed, now)
		}
	}
}

// 熔断器请求失败时调用该函数
func (cb *CircuitBreaker) onFailure(state State, now time.Time) {
	switch state {
	// 关闭状态下请求失败了
	case StateClosed:
		cb.counts.onFailure() // 更新计数
		// 如果回调函数 readyToTrip 返回 true
		// 因为一次失败可能不足以直接判定为需要熔断，所以可能失败多次后才会返回 true
		// 比如官方示例中设置的回调函数是：
		// st.ReadyToTrip = func(counts gobreaker.Counts) bool {
		//		failureRatio := float64(counts.TotalFailures) / float64(counts.Requests)
		//		return counts.Requests >= 3 && failureRatio >= 0.6
		//	}
		// 可以看到这里需要请求次数大于3，且总失败率大于等于 60% 才会返回 true
		if cb.readyToTrip(cb.counts) {
			cb.setState(StateOpen, now) // 变更熔断器为开启状态
		}
	case StateHalfOpen: // 半开状态下失败了，变更为开启状态
		cb.setState(StateOpen, now)
	}
}

// currentState 返回熔断器当前的状态，now 用来判断是否需要执行某些操作，这些操作包括：
// 1. 关闭状态下清空计数（如果设置了 interval 且达到了清空时间）
// 2. 开启状态转换为半开启状态（到达了转换时间）
func (cb *CircuitBreaker) currentState(now time.Time) (State, uint64) {
	// func toNewGeneration
	// case StateClosed:
	//		if cb.interval == 0 {
	//			cb.expiry = zero
	//		} else {
	//			cb.expiry = now.Add(cb.interval)
	//		}
	switch cb.state {
	// 如上面的注释代码所示，如果 cb.interval 为 0，那么 cb.expiry 会设置为 zero，
	// 此时下面的 if 条件就不满足了，关闭状态下也不会调用 cb.toNewGeneration 来清空计数
	// 如果设置了 cb.interval，那么会设置 cb.expiry 的时间，如果处于关闭状态且达到了
	// expiry 的时间，就会调用 cb.toNewGeneration 来清空计数
	case StateClosed:
		if !cb.expiry.IsZero() && cb.expiry.Before(now) {
			cb.toNewGeneration(now)
		}
	case StateOpen:
		// 超过了 expiry 的时间，可以切换到半开状态了
		if cb.expiry.Before(now) {
			cb.setState(StateHalfOpen, now)
		}
	}
	return cb.state, cb.generation
}

func (cb *CircuitBreaker) setState(state State, now time.Time) {
	if cb.state == state {
		return
	}

	prev := cb.state
	cb.state = state

	cb.toNewGeneration(now) // 设置新状态后更新计数

	if cb.onStateChange != nil {
		cb.onStateChange(cb.name, prev, state)
	}
}

// 进入一个新周期，会清空计数，并对 cb.expiry 进行更新
// 该函数会在 setState、currentState、NewCircuitBreaker 调用
func (cb *CircuitBreaker) toNewGeneration(now time.Time) {
	cb.generation++
	cb.counts.clear()

	var zero time.Time
	switch cb.state {
	case StateClosed:
		if cb.interval == 0 {
			cb.expiry = zero
		} else {
			cb.expiry = now.Add(cb.interval)
		}
	case StateOpen:
		cb.expiry = now.Add(cb.timeout) // 设置 open -> halfOpen 的绝对时间
	default: // StateHalfOpen
		cb.expiry = zero
	}
}
