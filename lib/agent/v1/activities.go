package agent

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"

	"github.com/anthropics/anthropic-sdk-go"
	"go.temporal.io/sdk/temporal"

	pb "github.com/jmbarzee/temporal-agent-sandbox/gen/agent/v1"
	sandboxv1 "github.com/jmbarzee/temporal-agent-sandbox/gen/sandbox/v1"
)

const defaultModel = "claude-opus-5-5"

// Activities call the Anthropic API. The client reads ANTHROPIC_API_KEY or an
// `ant auth login` profile; ANTHROPIC_MODEL overrides the model.
type Activities struct {
	client anthropic.Client
	model  string
}

func NewActivities() *Activities {
	model := os.Getenv("ANTHROPIC_MODEL")
	if model == "" {
		model = defaultModel
	}
	return &Activities{client: anthropic.NewClient(), model: model}
}

// CallModel sends the whole context and returns the next assistant message.
// The workflow owns the loop, so this is one request, never a tool runner.
func (a *Activities) CallModel(ctx context.Context, req *pb.CallModelInput) (*pb.CallModelOutput, error) {
	system, messages, err := toMessages(req.GetContext())
	if err != nil {
		return nil, temporal.NewNonRetryableApplicationError("bad context", "BadContext", err)
	}
	tools, err := toTools(req.GetTools())
	if err != nil {
		return nil, temporal.NewNonRetryableApplicationError("bad tools", "BadTools", err)
	}
	resp, err := a.client.Messages.New(ctx, anthropic.MessageNewParams{
		Model:     a.model,
		MaxTokens: 16000,
		System:    system,
		Messages:  messages,
		Tools:     tools,
	})
	if err != nil {
		return nil, err
	}

	reply := &pb.Message{Role: "assistant"}
	var text []string
	for _, block := range resp.Content {
		switch b := block.AsAny().(type) {
		case anthropic.TextBlock:
			text = append(text, b.Text)
		case anthropic.ToolUseBlock:
			reply.ToolCalls = append(reply.ToolCalls, &sandboxv1.ToolCall{Id: b.ID, Name: b.Name, Input: b.JSON.Input.Raw()})
		}
	}
	reply.Content = strings.Join(text, "\n")

	switch resp.StopReason {
	case anthropic.StopReasonToolUse:
		return &pb.CallModelOutput{Reply: reply}, nil
	case anthropic.StopReasonEndTurn:
		return &pb.CallModelOutput{Reply: reply, Done: len(reply.ToolCalls) == 0}, nil
	case anthropic.StopReasonRefusal:
		return nil, temporal.NewNonRetryableApplicationError(
			fmt.Sprintf("model refused: %s", resp.StopDetails.Explanation), "Refusal", nil)
	default:
		return nil, temporal.NewNonRetryableApplicationError(
			fmt.Sprintf("model stopped: %s", resp.StopReason), "Truncated", nil)
	}
}

// toMessages maps the agent's context onto API turns: the system message to
// the system prompt, assistant turns with tool_use blocks, tool turns as
// user turns of tool_result blocks.
func toMessages(c *pb.Context) ([]anthropic.TextBlockParam, []anthropic.MessageParam, error) {
	var system []anthropic.TextBlockParam
	var messages []anthropic.MessageParam
	for _, m := range c.GetMessages() {
		switch m.GetRole() {
		case "system":
			system = append(system, anthropic.TextBlockParam{Text: m.GetContent()})
		case "user":
			messages = append(messages, anthropic.NewUserMessage(anthropic.NewTextBlock(m.GetContent())))
		case "assistant":
			var blocks []anthropic.ContentBlockParamUnion
			if m.GetContent() != "" {
				blocks = append(blocks, anthropic.NewTextBlock(m.GetContent()))
			}
			for _, call := range m.GetToolCalls() {
				blocks = append(blocks, anthropic.NewToolUseBlock(call.GetId(), json.RawMessage(call.GetInput()), call.GetName()))
			}
			messages = append(messages, anthropic.NewAssistantMessage(blocks...))
		case "tool":
			var blocks []anthropic.ContentBlockParamUnion
			for _, out := range m.GetToolOutputs() {
				blocks = append(blocks, anthropic.NewToolResultBlock(out.GetToolCallId(), out.GetOutput(), out.GetExitCode() != 0))
			}
			messages = append(messages, anthropic.NewUserMessage(blocks...))
		default:
			return nil, nil, fmt.Errorf("unknown role %q", m.GetRole())
		}
	}
	return system, messages, nil
}

func toTools(t *pb.Tools) ([]anthropic.ToolUnionParam, error) {
	var tools []anthropic.ToolUnionParam
	for _, tool := range t.GetTools() {
		var schema struct {
			Properties map[string]any `json:"properties"`
			Required   []string       `json:"required"`
		}
		if err := json.Unmarshal([]byte(tool.GetInputSchema()), &schema); err != nil {
			return nil, fmt.Errorf("tool %s: %w", tool.GetName(), err)
		}
		param := anthropic.ToolParam{
			Name:        tool.GetName(),
			Description: anthropic.String(tool.GetDescription()),
			InputSchema: anthropic.ToolInputSchemaParam{Properties: schema.Properties, Required: schema.Required},
		}
		tools = append(tools, anthropic.ToolUnionParam{OfTool: &param})
	}
	return tools, nil
}
