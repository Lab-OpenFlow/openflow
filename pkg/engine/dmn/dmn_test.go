package dmn_test

import (
	"testing"

	"github.com/Lab-OpenFlow/openflow/pkg/engine/dmn"
)

func TestDMNHitPolicyFirst(t *testing.T) {
	dt := &dmn.DecisionTable{
		ID:        "credit_policy",
		HitPolicy: dmn.HitPolicyFirst,
		Inputs: []dmn.DMNInput{
			{Name: "score", Expression: "payload.score"},
			{Name: "income", Expression: "payload.income"},
		},
		Outputs: []dmn.DMNOutput{
			{Name: "decision"},
			{Name: "credit_limit"},
		},
		Rules: []dmn.DMNRule{
			{
				ID: "rule_vip",
				Conditions: map[string]interface{}{
					"score":  ">= 800",
					"income": ">= 10000",
				},
				Outputs: map[string]interface{}{
					"decision":     "APPROVED_VIP",
					"credit_limit": 50000,
				},
			},
			{
				ID: "rule_standard",
				Conditions: map[string]interface{}{
					"score":  ">= 600",
					"income": ">= 3000",
				},
				Outputs: map[string]interface{}{
					"decision":     "APPROVED_STANDARD",
					"credit_limit": 10000,
				},
			},
			{
				ID: "rule_fallback",
				Conditions: map[string]interface{}{
					"default": true,
				},
				Outputs: map[string]interface{}{
					"decision":     "REJECTED",
					"credit_limit": 0,
				},
			},
		},
	}

	// 1. VIP Match
	stateVIP := map[string]interface{}{
		"payload": map[string]interface{}{
			"score":  850,
			"income": 15000,
		},
	}
	resVIP, err := dt.Evaluate(stateVIP)
	if err != nil {
		t.Fatalf("VIP eval failed: %v", err)
	}
	if resVIP.Outputs["decision"] != "APPROVED_VIP" {
		t.Fatalf("expected APPROVED_VIP, got %v", resVIP.Outputs["decision"])
	}
	if resVIP.Outputs["credit_limit"] != 50000 {
		t.Fatalf("expected limit 50000, got %v", resVIP.Outputs["credit_limit"])
	}

	// 2. Standard Match
	stateStd := map[string]interface{}{
		"payload": map[string]interface{}{
			"score":  650,
			"income": 4000,
		},
	}
	resStd, err := dt.Evaluate(stateStd)
	if err != nil {
		t.Fatalf("Std eval failed: %v", err)
	}
	if resStd.Outputs["decision"] != "APPROVED_STANDARD" {
		t.Fatalf("expected APPROVED_STANDARD, got %v", resStd.Outputs["decision"])
	}

	// 3. Fallback / Rejected
	stateRej := map[string]interface{}{
		"payload": map[string]interface{}{
			"score":  500,
			"income": 2000,
		},
	}
	resRej, err := dt.Evaluate(stateRej)
	if err != nil {
		t.Fatalf("Rej eval failed: %v", err)
	}
	if resRej.Outputs["decision"] != "REJECTED" {
		t.Fatalf("expected REJECTED, got %v", resRej.Outputs["decision"])
	}
	if resRej.Outputs["credit_limit"] != 0 {
		t.Fatalf("expected limit 0, got %v", resRej.Outputs["credit_limit"])
	}
}

func TestDMNHitPolicyCollect(t *testing.T) {
	dt := &dmn.DecisionTable{
		ID:        "fraud_detection",
		HitPolicy: dmn.HitPolicyCollect,
		Inputs: []dmn.DMNInput{
			{Name: "amount", Expression: "payload.amount"},
			{Name: "country", Expression: "payload.country"},
			{Name: "is_new_device", Expression: "payload.is_new_device"},
		},
		Outputs: []dmn.DMNOutput{
			{Name: "risk_flag"},
		},
		Rules: []dmn.DMNRule{
			{
				ID: "high_amount",
				Conditions: map[string]interface{}{
					"amount": "> 5000",
				},
				Outputs: map[string]interface{}{
					"risk_flag": "HIGH_AMOUNT",
				},
			},
			{
				ID: "high_risk_country",
				Conditions: map[string]interface{}{
					"country": "in [\"XX\", \"YY\"]",
				},
				Outputs: map[string]interface{}{
					"risk_flag": "HIGH_RISK_GEO",
				},
			},
			{
				ID: "new_device_check",
				Conditions: map[string]interface{}{
					"is_new_device": "true",
				},
				Outputs: map[string]interface{}{
					"risk_flag": "NEW_DEVICE",
				},
			},
		},
	}

	state := map[string]interface{}{
		"payload": map[string]interface{}{
			"amount":        8000,
			"country":       "XX",
			"is_new_device": true,
		},
	}

	res, err := dt.Evaluate(state)
	if err != nil {
		t.Fatalf("Collect eval failed: %v", err)
	}

	if len(res.HitRules) != 3 {
		t.Fatalf("expected 3 hit rules, got %d: %v", len(res.HitRules), res.HitRules)
	}

	flags, ok := res.Outputs["risk_flag"].([]interface{})
	if !ok || len(flags) != 3 {
		t.Fatalf("expected 3 risk flags collected, got: %v", res.Outputs["risk_flag"])
	}
}

func TestDMNIntervalAndDash(t *testing.T) {
	dt := &dmn.DecisionTable{
		ID:        "discount_bracket",
		HitPolicy: dmn.HitPolicyFirst,
		Inputs: []dmn.DMNInput{
			{Name: "tier", Expression: "payload.tier"},
			{Name: "volume", Expression: "payload.volume"},
		},
		Outputs: []dmn.DMNOutput{
			{Name: "discount_pct"},
		},
		Rules: []dmn.DMNRule{
			{
				Conditions: map[string]interface{}{
					"tier":   "GOLD",
					"volume": "[1000..5000]",
				},
				Outputs: map[string]interface{}{
					"discount_pct": 20,
				},
			},
			{
				Conditions: map[string]interface{}{
					"tier":   "-", // Any tier
					"volume": "> 5000",
				},
				Outputs: map[string]interface{}{
					"discount_pct": 25,
				},
			},
		},
	}

	// 1. Bracket match
	state1 := map[string]interface{}{
		"payload": map[string]interface{}{
			"tier":   "GOLD",
			"volume": 2500,
		},
	}
	res1, err := dt.Evaluate(state1)
	if err != nil {
		t.Fatalf("eval failed: %v", err)
	}
	if res1.Outputs["discount_pct"] != 20 {
		t.Fatalf("expected discount 20, got %v", res1.Outputs["discount_pct"])
	}

	// 2. Dash match
	state2 := map[string]interface{}{
		"payload": map[string]interface{}{
			"tier":   "SILVER",
			"volume": 6000,
		},
	}
	res2, err := dt.Evaluate(state2)
	if err != nil {
		t.Fatalf("eval failed: %v", err)
	}
	if res2.Outputs["discount_pct"] != 25 {
		t.Fatalf("expected discount 25, got %v", res2.Outputs["discount_pct"])
	}
}
