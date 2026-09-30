package service

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// BPS 流的转写：文本与推理事件实时透传；原生工具事件（run_officejs 的参数增量与 item）全部扣住，
// 到 response.completed 拿到完整原生 item、翻译成客户端声明的工具后再一次性补发。非终态事件里的
// response.output 也把工具项过滤掉，客户端永远看不到原生的、未校验的工具参数。
// 上游长时间思考时不发事件，Codex 5 分钟没收到事件会断流；空闲满 keepalive 就重发一条
// response.in_progress。

const openAIBasisPointsKeepalive = 30 * time.Second

// openAIBasisPointsUpstreamSilence：上游连续这么久一个事件都不发就当死流的**兜底上限**，只在运维
// 关掉 stream_data_interval_timeout（填 0）时才生效；正常取值由 openAIBasisPointsUpstreamSilenceBudget
// 从那个配置项派生。
//
// 为什么必须有这一道：我们补的 response.in_progress 会刷新处理器的 lastReadAt
// （openai_gateway_response_handling.go，BPS 的 resp.Body 就是我们自己的 pipe），于是网关自己那道
// stream_data_interval_timeout **永远不会触发** —— 保活把它解除了武装，这里是唯一的空闲上界。
// 15 分钟比任何客户端的耐心都长，而槽位（acquireResponsesAccountSlot）全程持有、开这个开关的账号
// 又建议把并发设成 1，一发卡死就是这个账号整段零吞吐，所以不能把它当常规取值用。
// 变量而非常量：便于测试缩短。
var openAIBasisPointsUpstreamSilence = 15 * time.Minute

// openAIBasisPointsPeekSilence：确认上游接受之前的等待上限。这一段客户端还一个字节都没收到
// （连响应头都没写），目的是「快点判死、整条落回原路径」，不能复用 in-stream 的 15 分钟——
// 那比任何客户端的耐心都长。三分钟高于实测的 BPS 首事件延迟（xhigh 几十秒），又远低于 15 分钟。
var openAIBasisPointsPeekSilence = 3 * time.Minute

type openAIBasisPointsProtocolError struct{ error }

func (e openAIBasisPointsProtocolError) Unwrap() error { return e.error }

type openAIBasisPointsStreamBody struct {
	*io.PipeReader
	upstream io.ReadCloser
	once     sync.Once
	err      error
}

func (b *openAIBasisPointsStreamBody) closeUpstream() error {
	b.once.Do(func() { b.err = b.upstream.Close() })
	return b.err
}

func (b *openAIBasisPointsStreamBody) Close() error {
	return errors.Join(b.PipeReader.Close(), b.closeUpstream())
}

// bpsIsTerminalOutputSnapshot 报告这个终态事件是否带着一份值得翻译和补发的 output 快照。
// 与 isOpenAIWSTerminalEvent 的差别只有 response.failed —— 它的 output 恒为空。
func bpsIsTerminalOutputSnapshot(kind string) bool {
	switch kind {
	case "response.completed", "response.done", "response.incomplete",
		"response.cancelled", "response.canceled":
		return true
	default:
		return false
	}
}

// stream 返回转写后的响应体；关闭它会中断上游读取或阻塞中的管道写。
func (b *openAIBasisPointsBridge) stream(upstream io.ReadCloser, keepalive, silence time.Duration) io.ReadCloser {
	if silence <= 0 {
		silence = openAIBasisPointsUpstreamSilence
	}
	reader, writer := io.Pipe()
	body := &openAIBasisPointsStreamBody{PipeReader: reader, upstream: upstream}
	go func() {
		err := b.transform(upstream, writer, keepalive, silence)
		_ = body.closeUpstream()
		_ = writer.CloseWithError(err)
	}()
	return body
}

type openAIBasisPointsEmitter struct {
	mu       sync.Mutex
	writer   io.Writer
	sequence int
	lastAt   time.Time
	progress bpsObject
	terminal bool
}

func (e *openAIBasisPointsEmitter) emit(kind string, payload bpsObject) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.emitLocked(kind, payload)
}

func (e *openAIBasisPointsEmitter) emitLocked(kind string, payload bpsObject) error {
	payload["type"] = kind
	payload["sequence_number"] = e.sequence
	e.sequence++
	raw, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	e.lastAt = time.Now()
	_, err = fmt.Fprintf(e.writer, "event: %s\ndata: %s\n\n", kind, raw)
	return err
}

func (e *openAIBasisPointsEmitter) keepalive(every time.Duration) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.terminal || e.progress == nil || time.Since(e.lastAt) < every {
		return
	}
	_ = e.emitLocked("response.in_progress", bpsObject{"response": e.progress})
}

// 文本补发按消息与 content_index 记账，增量和两处终态快照共用同一状态。
type openAIBasisPointsTextPartKey struct {
	item         string
	contentIndex string
}

type openAIBasisPointsTextDeltaState struct {
	seen map[openAIBasisPointsTextPartKey]struct{}
}

func openAIBasisPointsTextPartIdentity(id string, outputIndex, contentIndex any) openAIBasisPointsTextPartKey {
	if outputIndex == nil {
		outputIndex = 0
	}
	if contentIndex == nil {
		contentIndex = 0
	}
	item := "id:" + id
	if id == "" {
		item = fmt.Sprintf("idx:%v", outputIndex)
	}
	return openAIBasisPointsTextPartKey{item: item, contentIndex: fmt.Sprint(contentIndex)}
}

func (s *openAIBasisPointsTextDeltaState) has(id string, outputIndex, contentIndex any) bool {
	// 上限满后停止补发，避免未记下的部件在后续快照中被重复发送。
	if len(s.seen) >= 1024 {
		return true
	}
	_, seen := s.seen[openAIBasisPointsTextPartIdentity(id, outputIndex, contentIndex)]
	return seen
}

func (s *openAIBasisPointsTextDeltaState) note(id string, outputIndex, contentIndex any) {
	if len(s.seen) >= 1024 {
		return
	}
	if s.seen == nil {
		s.seen = make(map[openAIBasisPointsTextPartKey]struct{})
	}
	s.seen[openAIBasisPointsTextPartIdentity(id, outputIndex, contentIndex)] = struct{}{}
}

func (s *openAIBasisPointsTextDeltaState) backfill(emitter *openAIBasisPointsEmitter, outputIndex any, item bpsObject) error {
	if bpsText(item["type"]) != "message" {
		return nil
	}
	if outputIndex == nil {
		outputIndex = 0
	}
	id := bpsText(item["id"])
	content, _ := item["content"].([]any)
	for i, raw := range content {
		part, _ := raw.(bpsObject)
		text := bpsText(part["text"])
		if bpsText(part["type"]) != "output_text" || text == "" || s.has(id, outputIndex, i) {
			continue
		}
		// 已有增量的部件不补；补过的部件也不能在 done/终态快照里再次发送。
		s.note(id, outputIndex, i)
		if err := emitter.emit("response.output_text.delta", bpsObject{
			"output_index": outputIndex, "item_id": id, "content_index": i, "delta": text,
		}); err != nil {
			return err
		}
	}
	return nil
}

func (b *openAIBasisPointsBridge) transform(upstream io.ReadCloser, writer io.Writer, keepalive, silence time.Duration) error {
	emitter := &openAIBasisPointsEmitter{writer: writer, lastAt: time.Now()}
	var lastUpstream atomic.Int64
	lastUpstream.Store(time.Now().UnixNano())
	var silenced atomic.Bool
	done := make(chan struct{})
	defer close(done)
	period := keepalive / 2
	if period <= 0 {
		period = time.Minute
	}
	go func() {
		ticker := time.NewTicker(period)
		defer ticker.Stop()
		for {
			select {
			case <-done:
				return
			case <-ticker.C:
				// silence 恒 > 0：唯一调用方 stream() 已经把 <=0 换成兜底上限。
				if time.Since(time.Unix(0, lastUpstream.Load())) > silence {
					silenced.Store(true)
					_ = upstream.Close()
					return
				}
				if keepalive > 0 {
					emitter.keepalive(keepalive)
				}
			}
		}
	}()
	emitted := map[string]bool{}
	pendingTools := map[string]bool{}
	emitTool := func(item bpsObject, index any) error {
		id := bpsText(item["id"])
		// 重复 id 以前是静默 return nil：第二个工具项一个事件都不发，却仍然留在
		// response.completed.output 里。客户端按 output_item.done 重建历史（Codex 的
		// completed.output 常为空，本仓库就是这么重建的）就会漏掉一次调用，下一轮历史里
		// 出现一个没有 output 的 function_call。判错交给上面的 protocol_error 合成。
		if emitted[id] {
			return errors.New("basispoints returned duplicate tool item id " + id)
		}
		emitted[id] = true
		field, prefix := "arguments", "response.function_call_arguments"
		if bpsText(item["type"]) == "custom_tool_call" {
			field, prefix = "input", "response.custom_tool_call_input"
		}
		added := make(bpsObject, len(item))
		for k, v := range item {
			added[k] = v
		}
		added[field], added["status"] = "", "in_progress"
		if err := emitter.emit("response.output_item.added", bpsObject{"output_index": index, "item": added}); err != nil {
			return err
		}
		if err := emitter.emit(prefix+".delta", bpsObject{"output_index": index, "item_id": id, "delta": item[field]}); err != nil {
			return err
		}
		if err := emitter.emit(prefix+".done", bpsObject{"output_index": index, "item_id": id, field: item[field]}); err != nil {
			return err
		}
		return emitter.emit("response.output_item.done", bpsObject{"output_index": index, "item": item})
	}
	// BPS 偶发只在快照中给正文；只补未发过增量的文本部件，避免漏字或重复。
	var textDeltas openAIBasisPointsTextDeltaState
	// 最近一个带 response 对象的事件：合成的失败事件要带上它的 id / model / usage，已消耗的额度才记得上。
	var lastResponse bpsObject
	sawBareError := false
	process := func(event string, data []byte) error {
		if string(data) == "[DONE]" {
			return nil
		}
		var payload bpsObject
		if bpsDecode(data, &payload) != nil || payload == nil {
			return errors.New("invalid basispoints SSE event")
		}
		kind := bpsText(payload["type"])
		if kind == "" {
			kind = event
		}
		if strings.HasPrefix(kind, "response.function_call_arguments.") || strings.HasPrefix(kind, "response.custom_tool_call_input.") {
			return nil
		}
		item, _ := payload["item"].(bpsObject)
		if kind == "response.output_text.delta" {
			textDeltas.note(bpsText(payload["item_id"]), payload["output_index"], payload["content_index"])
		}
		if kind == "response.output_item.done" && !bpsIsTool(item) {
			if err := textDeltas.backfill(emitter, payload["output_index"], item); err != nil {
				return err
			}
		}
		if kind == "response.output_item.added" && bpsIsTool(item) {
			return nil
		}
		if kind == "response.output_item.done" && bpsIsTool(item) {
			if len(pendingTools) >= 1024 {
				return errors.New("basispoints response contains too many tool items")
			}
			pendingTools[bpsText(item["call_id"])+"\x00"+bpsText(item["id"])] = true
			return nil
		}
		// 终态事件**没带** response 快照时，下面整段都不执行 —— 不检查扣留、不翻译、不补发，
		// 于是扣住的工具项被静默吞掉：客户端拿到一条「模型什么都没做」的 200，没有任何日志、
		// 错误码或读数。那正是下面那条注释描述的失败模式，而它原来只在快照存在时才防得住。
		if bpsIsTerminalOutputSnapshot(kind) && len(pendingTools) != 0 && payload["response"] == nil {
			return errors.New("basispoints terminal event carried no response snapshot")
		}
		if response, ok := payload["response"].(bpsObject); ok {
			lastResponse = response
			// 带 output 快照的终态事件都要走补发：只过滤不补发的话，扣住的工具项就被静默吞掉，
			// 客户端看到的是一条「模型什么都没做」的响应，既不会执行也不会报错。
			// cancelled / canceled / done 以前漏在这里，而本仓库另外四处终态表都含它们
			// （gateway_service.go:197、openai_gateway_messages.go:732、
			// openai_gateway_response_handling.go:1812/:2114）。failed 不进这条分支：
			// 它的 output 本来就是空的，翻译和补发都没有意义。
			if bpsIsTerminalOutputSnapshot(kind) {
				output, _ := response["output"].([]any)
				for _, raw := range output {
					item, _ := raw.(bpsObject)
					if bpsIsTool(item) {
						delete(pendingTools, bpsText(item["call_id"])+"\x00"+bpsText(item["id"]))
					}
				}
				// 截断 / 取消的那一轮里上游本来就可能不带上工具项，所以只有 completed 才当协议违规；
				// 其余终态丢掉剩余的扣留记录即可，别把一次正常截断或取消变成失败。
				if len(pendingTools) != 0 {
					if kind == "response.completed" {
						return errors.New("basispoints completed response omitted an original tool item")
					}
					pendingTools = map[string]bool{}
				}
				if err := b.translateResponse(response); err != nil {
					return err
				}
				// output_index 的语义就是「该 item 在 output 数组里的下标」，所以按真实下标补发、
				// 不重排数组。扣留策略必然让到达顺序和数组顺序打架（工具事件攒到终态才发），
				// 三个不变量里只能保一个 —— 保规范的这条：JaxsonWang 与 WsureDev 两家参考实现
				// 同样是 `for i, raw := range output { emitTool(item, i) }`，数组一个字节不动。
				// translateResponse 是原地替换（output[i] = translated），不丢项不换序，
				// 所以透传出去的那些事件自带的上游 output_index 与这里的下标仍然对得上。
				output, _ = response["output"].([]any)
				for i, raw := range output {
					item, _ := raw.(bpsObject)
					if !bpsIsTool(item) {
						// issue #12 的「正文只放在终态项里」有两种形态：output_item.done 里，
						// 和只出现在终态快照的 output[] 里（上游连 output_item.done 都不发）。
						// 两处共用部件级记账，保证终态快照不重复补发。
						if err := textDeltas.backfill(emitter, i, item); err != nil {
							return err
						}
						continue
					}
					if err := emitTool(item, i); err != nil {
						return err
					}
				}
			} else {
				output, _ := response["output"].([]any)
				filtered := make([]any, 0, len(output))
				for _, raw := range output {
					item, _ := raw.(bpsObject)
					if !bpsIsTool(item) {
						filtered = append(filtered, raw)
					}
				}
				response["output"] = filtered
				response["reasoning"] = bpsObject{"effort": b.effort}
				if kind == "response.created" || kind == "response.in_progress" {
					emitter.mu.Lock()
					emitter.progress = response
					emitter.mu.Unlock()
				}
			}
		}
		// 裸 error 之后上游通常还会补一条带 usage 的 response.failed，与原路径一样继续读。
		if kind == "error" {
			sawBareError = true
		}
		terminal := isOpenAIWSTerminalEvent(kind)
		emitter.mu.Lock()
		defer emitter.mu.Unlock()
		if terminal {
			emitter.terminal = true
		}
		return emitter.emitLocked(kind, payload)
	}
	err := readOpenAIBasisPointsEvents(upstream, func(event string, data []byte) error {
		lastUpstream.Store(time.Now().UnixNano())
		if emitter.terminal {
			return io.EOF
		}
		if err := process(event, data); err != nil {
			return openAIBasisPointsProtocolError{err}
		}
		if emitter.terminal {
			return io.EOF
		}
		return nil
	})
	fail := func(code, errType, message string) error {
		emitter.mu.Lock()
		emitter.terminal = true
		emitter.mu.Unlock()
		response := bpsObject{"status": "failed", "output": []any{}, "error": bpsObject{"code": code, "type": errType, "message": message}}
		for _, key := range []string{"id", "model", "usage"} {
			if value, ok := lastResponse[key]; ok && value != nil {
				response[key] = value
			}
		}
		return emitter.emit("response.failed", bpsObject{"response": response})
	}
	if silenced.Load() && !emitter.terminal {
		return fail("basispoints_upstream_timeout", "server_error", fmt.Sprintf("Basis Points upstream sent no event for %s", silence))
	}
	if err != nil && !errors.Is(err, io.EOF) {
		var invalid openAIBasisPointsProtocolError
		if errors.Is(err, io.ErrClosedPipe) || !errors.As(err, &invalid) {
			return err
		}
		// 协议违规是这条请求自己的确定性失败：标成 invalid_request，处理器不会拿它换号重跑。
		return fail("basispoints_protocol_error", "invalid_request_error", "Basis Points tool relay failed; no tool was executed ("+err.Error()+")")
	}
	if !emitter.terminal {
		if sawBareError {
			return nil
		}
		return io.ErrUnexpectedEOF
	}
	return nil
}

func readOpenAIBasisPointsEvents(reader io.Reader, consume func(string, []byte) error) error {
	scanner := bufio.NewScanner(reader)
	scanner.Buffer(make([]byte, 64*1024), 16<<20)
	var data strings.Builder
	event := ""
	flush := func() error {
		if data.Len() == 0 {
			event = ""
			return nil
		}
		err := consume(event, []byte(strings.TrimSuffix(data.String(), "\n")))
		data.Reset()
		event = ""
		return err
	}
	for scanner.Scan() {
		line := strings.TrimSuffix(scanner.Text(), "\r")
		switch {
		case line == "":
			if err := flush(); err != nil {
				return err
			}
		case strings.HasPrefix(line, "event:"):
			event = strings.TrimSpace(strings.TrimPrefix(line, "event:"))
		case strings.HasPrefix(line, "data:"):
			_, _ = data.WriteString(strings.TrimPrefix(strings.TrimPrefix(line, "data:"), " "))
			_ = data.WriteByte('\n')
			if data.Len() > 16<<20 {
				return openAIBasisPointsProtocolError{errors.New("basispoints SSE event exceeds 16 MiB")}
			}
		}
	}
	if err := scanner.Err(); err != nil {
		if errors.Is(err, bufio.ErrTooLong) {
			return openAIBasisPointsProtocolError{errors.New("basispoints SSE line exceeds 16 MiB")}
		}
		return err
	}
	return flush()
}
