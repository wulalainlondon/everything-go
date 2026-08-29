package automation

import (
	"context"
	"errors"
	"os"
	"strings"
)

// BootstrapAccountsFromEnv creates conservative account metadata for credentials
// already supplied by the signed launcher. Existing records are never
// overwritten. Polling/webhooks remain disabled unless their complete runtime
// configuration is present.
func BootstrapAccountsFromEnv(ctx context.Context, store *Store) (int, error) {
	graphVersionReady := strings.TrimSpace(os.Getenv("META_GRAPH_API_VERSION")) != ""
	webhookReady := strings.TrimSpace(os.Getenv("META_APP_SECRET")) != "" && strings.TrimSpace(os.Getenv("META_WEBHOOK_VERIFY_TOKEN")) != ""
	candidates := []Account{
		{ID: "facebook-judge", Provider: "meta.facebook", ExternalAccountID: strings.TrimSpace(os.Getenv("FB_JUDGE_PAGE_ID")),
			DisplayName: "實習判官 Facebook", CredentialRef: "env:FB_JUDGE_PAGE_TOKEN", AppSecretRef: "env:META_APP_SECRET",
			VerifyTokenRef: "env:META_WEBHOOK_VERIFY_TOKEN", Enabled: true, WebhookEnabled: webhookReady,
			PollEnabled: graphVersionReady, PollIntervalSeconds: 300},
		{ID: "facebook-ulala", Provider: "meta.facebook", ExternalAccountID: strings.TrimSpace(os.Getenv("FB_ULALA_PAGE_ID")),
			DisplayName: "Wulala Facebook", CredentialRef: "env:FB_ULALA_PAGE_TOKEN", AppSecretRef: "env:META_APP_SECRET",
			VerifyTokenRef: "env:META_WEBHOOK_VERIFY_TOKEN", Enabled: true, WebhookEnabled: webhookReady,
			PollEnabled: graphVersionReady, PollIntervalSeconds: 300},
		{ID: "threads-primary", Provider: "meta.threads", ExternalAccountID: strings.TrimSpace(os.Getenv("THREADS_USER_ID")),
			DisplayName: "Wulala Threads", CredentialRef: "env:THREADS_TOKEN", Enabled: false,
			PollEnabled: false, PollIntervalSeconds: 300},
	}
	created := 0
	for _, account := range candidates {
		if account.ExternalAccountID == "" {
			continue
		}
		if _, err := store.GetAccount(ctx, account.ID); err == nil {
			continue
		} else if !errors.Is(err, ErrNotFound) {
			return created, err
		}
		if _, _, err := store.UpsertAccount(ctx, account, 0); err != nil {
			return created, err
		}
		created++
	}
	return created, nil
}
