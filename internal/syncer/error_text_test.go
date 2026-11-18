package syncer

import (
	"strings"
	"testing"
)

func TestCleanHTTPBodyStripsEdgePage(t *testing.T) {
	body := `<html>
<head><title>413 Payload Too Large</title></head>
<body>
<center><h1>413 Payload Too Large</h1></center>
<hr><center>cloudflare</center>
<script type="module" src="https://static.cloudflareinsights.com/beacon.min.js/x" data-cf-beacon='{"token":"abc"}'></script>
</body></html>`
	got := cleanHTTPBody(body)
	if strings.Contains(got, "<") || strings.Contains(got, "script") || strings.Contains(got, "beacon") {
		t.Fatalf("cleaned body still carries markup: %q", got)
	}
	if !strings.Contains(got, "413 Payload Too Large") {
		t.Fatalf("cleaned body lost its meaning: %q", got)
	}
	if strings.Contains(got, "\n") {
		t.Fatalf("cleaned body should be one line: %q", got)
	}
}

func TestRemoteStatusErrorExplainsTunnelLimits(t *testing.T) {
	err := remoteStatusError("上传文件", 413, "<html><body>413 Payload Too Large</body></html>")
	if !strings.Contains(err.Error(), "100MB") {
		t.Fatalf("413 message should name the tunnel's body cap: %v", err)
	}
	err = remoteStatusError("上传文件", 524, "error code: 524")
	if !strings.Contains(err.Error(), "524") {
		t.Fatalf("524 message lost its code: %v", err)
	}
}
