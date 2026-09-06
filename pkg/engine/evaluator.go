package engine

import (
	"bytes"
	"fmt"
	"regexp"
	"strings"
	"sync"
	"text/template"

	"github.com/Lab-OpenFlow/openflow/pkg/security"
	"github.com/expr-lang/expr"
	"github.com/expr-lang/expr/vm"
)

// Evaluator evaluates dynamic expressions and template strings against workflow context.
type Evaluator struct {
	conditionCache sync.Map // map[string]*vm.Program
	exprCache      sync.Map // map[string]*vm.Program
	templateCache  sync.Map // map[string]*template.Template
}

// NewEvaluator creates a new expression evaluator with initialized caches.
func NewEvaluator() *Evaluator {
	return &Evaluator{}
}

// EvalCondition evaluates a boolean expression using expr-lang.
func (e *Evaluator) EvalCondition(expression string, env map[string]interface{}) (bool, error) {
	trimmed := strings.TrimSpace(expression)
	if trimmed == "" {
		return true, nil
	}

	var program *vm.Program
	if cached, ok := e.conditionCache.Load(trimmed); ok {
		program = cached.(*vm.Program)
	} else {
		compiled, err := expr.Compile(trimmed, expr.Env(env), expr.AsBool(), expr.Function("secret", func(params ...interface{}) (interface{}, error) {
			if len(params) == 0 {
				return "", fmt.Errorf("secret() requires a key argument")
			}
			key := fmt.Sprintf("%v", params[0])
			if security.GlobalVault != nil {
				return security.GlobalVault.GetSecret(key)
			}
			return "", nil
		}))
		if err != nil {
			return false, fmt.Errorf("failed to compile condition '%s': %w", expression, err)
		}
		e.conditionCache.Store(trimmed, compiled)
		program = compiled
	}

	output, err := expr.Run(program, env)
	if err != nil {
		return false, fmt.Errorf("failed to evaluate condition '%s': %w", expression, err)
	}

	result, ok := output.(bool)
	if !ok {
		return false, fmt.Errorf("expression '%s' did not return boolean", expression)
	}

	return result, nil
}

// EvalExpression evaluates an expression that can return any type.
func (e *Evaluator) EvalExpression(expression string, env map[string]interface{}) (interface{}, error) {
	trimmed := strings.TrimSpace(expression)
	if trimmed == "" {
		return env, nil
	}

	if val, ok := e.fastResolvePath(trimmed, env); ok {
		return val, nil
	}

	var program *vm.Program
	if cached, ok := e.exprCache.Load(trimmed); ok {
		program = cached.(*vm.Program)
	} else {
		compiled, err := expr.Compile(trimmed, expr.Env(env), expr.Function("secret", func(params ...interface{}) (interface{}, error) {
			if len(params) == 0 {
				return "", fmt.Errorf("secret() requires a key argument")
			}
			key := fmt.Sprintf("%v", params[0])
			if security.GlobalVault != nil {
				return security.GlobalVault.GetSecret(key)
			}
			return "", nil
		}))
		if err != nil {
			return nil, fmt.Errorf("failed to compile expression '%s': %w", expression, err)
		}
		e.exprCache.Store(trimmed, compiled)
		program = compiled
	}

	output, err := expr.Run(program, env)
	if err != nil {
		return nil, fmt.Errorf("failed to evaluate expression '%s': %w", expression, err)
	}

	return output, nil
}

func (e *Evaluator) fastResolvePath(exprStr string, env map[string]interface{}) (interface{}, bool) {
	for i := 0; i < len(exprStr); i++ {
		c := exprStr[i]
		if (c < 'a' || c > 'z') && (c < 'A' || c > 'Z') && (c < '0' || c > '9') && c != '.' && c != '_' {
			return nil, false
		}
	}

	parts := strings.Split(exprStr, ".")
	if len(parts) == 0 {
		return nil, false
	}

	var current interface{} = env
	for _, part := range parts {
		m, ok := current.(map[string]interface{})
		if !ok {
			return nil, false
		}
		val, exists := m[part]
		if !exists {
			return nil, false
		}
		current = val
	}
	return current, true
}

var templateRegex = regexp.MustCompile(`\{\{([^{}]+)\}\}`)

// InterpolateString replaces template placeholders like {{.payload.id}} or {{ secret "api/token" }}.
func (e *Evaluator) InterpolateString(text string, env map[string]interface{}) (string, error) {
	if !templateRegex.MatchString(text) {
		return text, nil
	}

	funcMap := template.FuncMap{
		"secret": func(key string) string {
			if security.GlobalVault != nil {
				sec, err := security.GlobalVault.GetSecret(key)
				if err == nil {
					return sec
				}
			}
			return ""
		},
	}

	var tmpl *template.Template
	if cached, ok := e.templateCache.Load(text); ok {
		tmpl = cached.(*template.Template)
	} else {
		parsed, err := template.New("eval").Funcs(funcMap).Option("missingkey=zero").Parse(text)
		if err != nil {
			return text, fmt.Errorf("template parse error: %w", err)
		}
		e.templateCache.Store(text, parsed)
		tmpl = parsed
	}

	var buf bytes.Buffer
	if err := tmpl.Execute(&buf, env); err != nil {
		return text, fmt.Errorf("template execute error: %w", err)
	}

	return buf.String(), nil
}

// InterpolateMap recursively interpolates strings in maps.
func (e *Evaluator) InterpolateMap(input map[string]interface{}, env map[string]interface{}) (map[string]interface{}, error) {
	if input == nil {
		return nil, nil
	}

	output := make(map[string]interface{}, len(input))
	for k, v := range input {
		interpolatedVal, err := e.interpolateValue(v, env)
		if err != nil {
			return nil, fmt.Errorf("field '%s': %w", k, err)
		}
		output[k] = interpolatedVal
	}

	return output, nil
}

func (e *Evaluator) interpolateValue(val interface{}, env map[string]interface{}) (interface{}, error) {
	switch v := val.(type) {
	case string:
		return e.InterpolateString(v, env)
	case map[string]interface{}:
		return e.InterpolateMap(v, env)
	case []interface{}:
		resList := make([]interface{}, len(v))
		for i, item := range v {
			interpItem, err := e.interpolateValue(item, env)
			if err != nil {
				return nil, err
			}
			resList[i] = interpItem
		}
		return resList, nil
	default:
		return val, nil
	}
}

// EvaluateMapping evaluates a map of expressions against the environment.
func (e *Evaluator) EvaluateMapping(mapping map[string]interface{}, env map[string]interface{}) map[string]interface{} {
	result := make(map[string]interface{})
	for k, v := range mapping {
		if exprStr, ok := v.(string); ok {
			if val, err := e.EvalExpression(exprStr, env); err == nil {
				result[k] = val
			} else {
				result[k] = v
			}
		} else {
			result[k] = v
		}
	}
	return result
}
