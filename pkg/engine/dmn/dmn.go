package dmn

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"github.com/Lab-OpenFlow/openflow/pkg/engine"
)

// HitPolicy specifies how decision table rules are selected and combined.
type HitPolicy string

const (
	// HitPolicyFirst returns the outputs of the first satisfied rule.
	HitPolicyFirst HitPolicy = "first"
	// HitPolicyCollect returns a collection of outputs from all satisfied rules.
	HitPolicyCollect HitPolicy = "collect"
	// HitPolicyRuleOrder returns outputs for all satisfied rules in the order defined.
	HitPolicyRuleOrder HitPolicy = "rule_order"
)

// DMNInput defines an input expression evaluated before rule testing.
type DMNInput struct {
	Name       string `json:"name" yaml:"name"`
	Expression string `json:"expression" yaml:"expression"`
}

// DMNOutput defines an output column name.
type DMNOutput struct {
	Name string `json:"name" yaml:"name"`
}

// DMNRule defines a single row in the decision table.
type DMNRule struct {
	ID          string                 `json:"id,omitempty" yaml:"id,omitempty"`
	Description string                 `json:"description,omitempty" yaml:"description,omitempty"`
	Conditions  map[string]interface{} `json:"conditions" yaml:"conditions"`
	Outputs     map[string]interface{} `json:"outputs" yaml:"outputs"`
}

// DecisionTable represents a complete DMN Decision Table definition.
type DecisionTable struct {
	ID          string      `json:"id,omitempty" yaml:"id,omitempty"`
	Name        string      `json:"name,omitempty" yaml:"name,omitempty"`
	HitPolicy   HitPolicy   `json:"hit_policy,omitempty" yaml:"hit_policy,omitempty"`
	Inputs      []DMNInput  `json:"inputs" yaml:"inputs"`
	Outputs     []DMNOutput `json:"outputs" yaml:"outputs"`
	Rules       []DMNRule   `json:"rules" yaml:"rules"`
}

// Result contains the output of a decision table evaluation.
type Result struct {
	HitRules []string               `json:"hit_rules"`
	Outputs  map[string]interface{} `json:"outputs"`
	AllHits  []map[string]interface{} `json:"all_hits,omitempty"`
}

// Evaluate evaluates the decision table against the provided state.
func (dt *DecisionTable) Evaluate(state map[string]interface{}) (*Result, error) {
	evaluator := engine.NewEvaluator()

	// 1. Resolve input values from state expressions
	resolvedInputs := make(map[string]interface{})
	for _, in := range dt.Inputs {
		expr := in.Expression
		if expr == "" {
			expr = in.Name
		}
		val, err := evaluator.EvalExpression(expr, state)
		if err != nil {
			// Fallback: check direct key access if expression evaluation returns error
			if directVal, ok := state[expr]; ok {
				val = directVal
			} else {
				val = nil
			}
		}
		resolvedInputs[in.Name] = val
	}

	hitPolicy := dt.HitPolicy
	if hitPolicy == "" {
		hitPolicy = HitPolicyFirst
	}

	var hitRuleIDs []string
	var allHits []map[string]interface{}

	// 2. Evaluate rules in sequential order
	for idx, rule := range dt.Rules {
		ruleID := rule.ID
		if ruleID == "" {
			ruleID = fmt.Sprintf("rule_%d", idx+1)
		}

		matched, err := matchRule(rule.Conditions, resolvedInputs)
		if err != nil {
			return nil, fmt.Errorf("evaluating rule %s failed: %w", ruleID, err)
		}

		if matched {
			hitRuleIDs = append(hitRuleIDs, ruleID)
			allHits = append(allHits, rule.Outputs)

			if hitPolicy == HitPolicyFirst {
				return &Result{
					HitRules: hitRuleIDs,
					Outputs:  rule.Outputs,
					AllHits:  allHits,
				}, nil
			}
		}
	}

	// For Collect or RuleOrder policies, combine outputs
	combinedOutputs := make(map[string]interface{})
	if len(allHits) > 0 {
		if hitPolicy == HitPolicyCollect {
			for _, hit := range allHits {
				for k, v := range hit {
					if existing, ok := combinedOutputs[k]; ok {
						if arr, ok := existing.([]interface{}); ok {
							combinedOutputs[k] = append(arr, v)
						} else {
							combinedOutputs[k] = []interface{}{existing, v}
						}
					} else {
						combinedOutputs[k] = []interface{}{v}
					}
				}
			}
		} else {
			// RuleOrder: use the last matched rule as top-level output, with all hits in AllHits
			combinedOutputs = allHits[0]
		}
	}

	return &Result{
		HitRules: hitRuleIDs,
		Outputs:  combinedOutputs,
		AllHits:  allHits,
	}, nil
}

// matchRule checks if all conditions for a rule are satisfied by the resolved inputs.
func matchRule(conditions map[string]interface{}, inputs map[string]interface{}) (bool, error) {
	// Check for catch-all default
	if defVal, ok := conditions["default"]; ok {
		if b, ok := defVal.(bool); ok && b {
			return true, nil
		}
	}

	for inputName, conditionVal := range conditions {
		if inputName == "default" {
			continue
		}

		inputValue, hasInput := inputs[inputName]
		if !hasInput {
			inputValue = nil
		}

		condStr := fmt.Sprintf("%v", conditionVal)
		condStr = strings.TrimSpace(condStr)

		// "-" means "Any" / Don't care in DMN
		if condStr == "-" || condStr == "" {
			continue
		}

		matched, err := evaluateCondition(condStr, inputValue)
		if err != nil {
			return false, fmt.Errorf("condition on %s (%s): %w", inputName, condStr, err)
		}
		if !matched {
			return false, nil
		}
	}

	return true, nil
}

// evaluateCondition checks if inputValue satisfies condition string.
func evaluateCondition(cond string, value interface{}) (bool, error) {
	// List inclusion: in ["A", "B", "C"]
	if strings.HasPrefix(strings.ToLower(cond), "in ") {
		listStr := strings.TrimSpace(cond[3:])
		listStr = strings.TrimPrefix(listStr, "[")
		listStr = strings.TrimSuffix(listStr, "]")
		parts := strings.Split(listStr, ",")
		valStr := fmt.Sprintf("%v", value)
		for _, p := range parts {
			cleaned := strings.Trim(strings.TrimSpace(p), "\"'")
			if strings.EqualFold(cleaned, valStr) {
				return true, nil
			}
		}
		return false, nil
	}

	// Interval check: [min..max] or (min..max)
	if (strings.HasPrefix(cond, "[") || strings.HasPrefix(cond, "(")) && strings.Contains(cond, "..") {
		return evaluateInterval(cond, value)
	}

	// Comparison operators: >=, <=, >, <, !=, ==
	compOps := []string{">=", "<=", "!=", "==", ">", "<"}
	for _, op := range compOps {
		if strings.HasPrefix(cond, op) {
			targetStr := strings.TrimSpace(strings.TrimPrefix(cond, op))
			targetStr = strings.Trim(targetStr, "\"'")
			return compareValues(op, value, targetStr)
		}
	}

	// Direct equality match
	cleanCond := strings.Trim(cond, "\"'")
	valStr := fmt.Sprintf("%v", value)
	if strings.EqualFold(cleanCond, valStr) {
		return true, nil
	}

	// If condition is a boolean
	if (cond == "true" || cond == "false") && value != nil {
		bVal, ok := value.(bool)
		if ok {
			return (cond == "true" && bVal) || (cond == "false" && !bVal), nil
		}
	}

	return false, nil
}

// compareValues compares inputValue against target using numeric or string comparison.
func compareValues(op string, value interface{}, target string) (bool, error) {
	// Try float comparison first
	targetNum, targetIsNum := parseNumber(target)
	valNum, valIsNum := parseNumber(value)

	if targetIsNum && valIsNum {
		switch op {
		case ">":
			return valNum > targetNum, nil
		case ">=":
			return valNum >= targetNum, nil
		case "<":
			return valNum < targetNum, nil
		case "<=":
			return valNum <= targetNum, nil
		case "==":
			return valNum == targetNum, nil
		case "!=":
			return valNum != targetNum, nil
		}
	}

	// String comparison
	valStr := fmt.Sprintf("%v", value)
	switch op {
	case "==":
		return strings.EqualFold(valStr, target), nil
	case "!=":
		return !strings.EqualFold(valStr, target), nil
	case ">":
		return valStr > target, nil
	case ">=":
		return valStr >= target, nil
	case "<":
		return valStr < target, nil
	case "<=":
		return valStr <= target, nil
	}

	return false, nil
}

// evaluateInterval parses and checks intervals like [100..500] or (0..100].
func evaluateInterval(cond string, value interface{}) (bool, error) {
	valNum, isNum := parseNumber(value)
	if !isNum {
		return false, nil
	}

	inclusiveStart := strings.HasPrefix(cond, "[")
	inclusiveEnd := strings.HasSuffix(cond, "]")

	trimmed := cond[1 : len(cond)-1]
	parts := strings.Split(trimmed, "..")
	if len(parts) != 2 {
		return false, fmt.Errorf("invalid interval format: %s", cond)
	}

	startNum, err1 := strconv.ParseFloat(strings.TrimSpace(parts[0]), 64)
	endNum, err2 := strconv.ParseFloat(strings.TrimSpace(parts[1]), 64)
	if err1 != nil || err2 != nil {
		return false, fmt.Errorf("invalid interval numbers: %s", cond)
	}

	startOk := (inclusiveStart && valNum >= startNum) || (!inclusiveStart && valNum > startNum)
	endOk := (inclusiveEnd && valNum <= endNum) || (!inclusiveEnd && valNum < endNum)

	return startOk && endOk, nil
}

var numRegex = regexp.MustCompile(`^-?\d+(\.\d+)?$`)

func parseNumber(v interface{}) (float64, bool) {
	if v == nil {
		return 0, false
	}
	switch n := v.(type) {
	case int:
		return float64(n), true
	case int64:
		return float64(n), true
	case int32:
		return float64(n), true
	case float64:
		return n, true
	case float32:
		return float64(n), true
	case string:
		clean := strings.TrimSpace(n)
		if numRegex.MatchString(clean) {
			f, err := strconv.ParseFloat(clean, 64)
			return f, err == nil
		}
	}
	return 0, false
}
