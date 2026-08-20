// Contract: contracts/sidecar.md — rules A28 to A30, the invoke a service-bound step executes as.

package steps

import (
	"context"
	"errors"
	"time"

	bbsdk "github.com/Clifford-Foster/slate-sdk-go/bbsdk"
)

// invokeCause is one row of rule 13's mapping from the invoke vocabulary onto the rule-3 taxonomy.
type invokeCause struct {
	cause     string
	retryable bool
}

// invokeCauses is rule 13's table verbatim. The four declaration and resolution codes are
// configuration bugs, never retried into a broken setup.
var invokeCauses = map[string]invokeCause{
	"INVOKE_TIMEOUT":         {CauseTimeout, true},
	"INVOKE_NO_RESPONDERS":   {CauseRemote, true},
	"INVOKE_SERVICE_ERROR":   {CauseRemote, false},
	"INVOKE_NOT_DECLARED":    {CauseInternal, false},
	"SERVICE_UNKNOWN":        {CauseInternal, false},
	"ENDPOINT_UNKNOWN":       {CauseInternal, false},
	"INVOKE_PAYLOAD_INVALID": {CauseInternal, false},
}

// invokeFallback is rule 3's default for an invoke code outside the table — internal and never a
// fabricated transient.
var invokeFallback = invokeCause{CauseInternal, false}

// invokeStep executes a service-bound step as one invoke on the composite's own sidecar (rule 13):
// the input dict is the payload verbatim, the reply dict is the output verbatim.
func invokeStep(ctx context.Context, entry *boundStep, input map[string]any) (map[string]any, *StepError) {
	target, invoker := entry.spec.Service, entry.invoker
	if target == nil || invoker == nil {
		return nil, stepError(CauseInternal, false, nil,
			"step %q is bound %q but the runner was given no invoke transport for it", entry.spec.Alias, bindingNATS)
	}
	options := []bbsdk.InvokeOption{}
	if entry.spec.TimeoutS > 0 {
		// The step's timeout_s is transmitted with the invoke and kept as the local budget; whichever
		// expires first produces the identical timeout StepError.
		options = append(options, bbsdk.WithInvokeTimeout(time.Duration(entry.spec.TimeoutS)*time.Second))
	}
	reply, err := invoker.Invoke(ctx, target.Name, target.Endpoint, input, options...)
	if err == nil {
		return reply, nil
	}
	var failure *bbsdk.SidecarError
	if !errors.As(err, &failure) {
		return nil, normalize(entry.spec.Alias, err)
	}
	mapped, known := invokeCauses[failure.Code]
	if !known {
		mapped = invokeFallback
	}
	return nil, stepError(mapped.cause, mapped.retryable, err,
		"step %q invoke of %s.%s failed (%s): %s",
		entry.spec.Alias, target.Name, target.Endpoint, failure.Code, failure.Message)
}
