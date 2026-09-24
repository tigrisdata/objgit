// Package webhook delivers signed push events using per-repository settings
// stored alongside daemon state in the bucket.
package webhook

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/netip"
	"net/url"
	"os"
	"path"
	"strings"
	"time"

	"github.com/go-git/go-billy/v6"
	pushv1 "github.com/tigrisdata/objgit/gen/tigrisdata/objgit/events/push/v1"
	settingsv1 "github.com/tigrisdata/objgit/gen/tigrisdata/objgit/webhooks/v1"
	"github.com/tigrisdata/objgit/internal/repofs"
	"google.golang.org/protobuf/encoding/protojson"
)

const (
	deliveryTimeout = 5 * time.Second
	attemptTimeout  = 2 * time.Second
	maxAttempts     = 3
	maxConfigBytes  = 1 << 20
)

// Client is an immutable collection of webhook destinations. Its zero value
// has no destinations and is safe to use.
type Client struct {
	repositories map[string]destination
}

type destination struct {
	url    string
	secret []byte
	client *http.Client
}

// SettingsPath returns the daemon-state key for a canonical repository path.
// The repository components must not escape the webhooks directory.
func SettingsPath(repo string) (string, error) {
	ref, err := repofs.Parse(repo)
	if err != nil || ref.Path() != repo || ref.OrgID == "." || ref.OrgID == ".." || ref.Name == "." || ref.Name == ".." {
		return "", fmt.Errorf("invalid repository %q", repo)
	}
	return path.Join(".objgit", "webhooks", ref.OrgID, ref.Name, "settings.json"), nil
}

// Load reads one repository's ProtoJSON settings from the daemon's bucket
// filesystem. A missing file disables delivery for that repository. Callers
// load once per push so the destination and secret stay fixed for its events.
func Load(fs billy.Filesystem, repo string) (*Client, error) {
	c := &Client{repositories: make(map[string]destination)}
	settings, err := ReadSettings(fs, repo)
	if err != nil || settings == nil {
		return c, err
	}
	settingsPath, _ := SettingsPath(repo)
	if settings.GetSecret() == "" {
		return nil, fmt.Errorf("webhook settings %s have no secret", settingsPath)
	}
	u, loopback, err := validateURL(settings.GetUrl())
	if err != nil {
		return nil, fmt.Errorf("webhook settings %s: %w", settingsPath, err)
	}
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.Proxy = nil
	transport.DialContext = safeDialContext(u.Hostname(), loopback)
	transport.MaxIdleConnsPerHost = 2
	c.repositories[repo] = destination{
		url:    u.String(),
		secret: []byte(settings.GetSecret()),
		client: &http.Client{
			Timeout:   attemptTimeout,
			Transport: transport,
			CheckRedirect: func(*http.Request, []*http.Request) error {
				return http.ErrUseLastResponse
			},
		},
	}
	return c, nil
}

// ReadSettings returns the raw settings for an admin query. A missing file
// returns nil. Callers must authorize before calling it or serializing it.
func ReadSettings(fs billy.Filesystem, repo string) (*settingsv1.Settings, error) {
	settingsPath, err := SettingsPath(repo)
	if err != nil {
		return nil, err
	}
	if fs == nil {
		return nil, nil
	}
	f, err := fs.Open(settingsPath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil
		}
		return nil, fmt.Errorf("open webhook settings %s: %w", settingsPath, err)
	}
	defer f.Close()

	data, err := io.ReadAll(io.LimitReader(f, maxConfigBytes+1))
	if err != nil {
		return nil, fmt.Errorf("read webhook settings %s: %w", settingsPath, err)
	}
	if len(data) > maxConfigBytes {
		return nil, fmt.Errorf("webhook settings %s exceed 1 MiB", settingsPath)
	}
	var settings settingsv1.Settings
	if err := protojson.Unmarshal(data, &settings); err != nil {
		return nil, fmt.Errorf("decode webhook settings %s: %w", settingsPath, err)
	}
	return &settings, nil
}

// NewSecret returns a random signing secret: 32 bytes, hex encoded.
func NewSecret() string {
	b := make([]byte, 32)
	_, _ = rand.Read(b) // crypto/rand.Read never returns an error.
	return hex.EncodeToString(b)
}

// WriteSettings validates settings and stores them as repo's ProtoJSON
// settings object, replacing any earlier one. The next push reads them.
func WriteSettings(fs billy.Filesystem, repo string, settings *settingsv1.Settings) error {
	settingsPath, err := SettingsPath(repo)
	if err != nil {
		return err
	}
	if settings.GetSecret() == "" {
		return errors.New("webhook settings have no secret")
	}
	if _, _, err := validateURL(settings.GetUrl()); err != nil {
		return err
	}
	body, err := protojson.Marshal(settings)
	if err != nil {
		return fmt.Errorf("encode webhook settings: %w", err)
	}
	if err := fs.MkdirAll(path.Dir(settingsPath), 0o700); err != nil && !errors.Is(err, os.ErrExist) {
		return fmt.Errorf("create webhook settings directory: %w", err)
	}
	f, err := fs.Create(settingsPath)
	if err != nil {
		return fmt.Errorf("create webhook settings %s: %w", settingsPath, err)
	}
	if _, err := f.Write(body); err != nil {
		f.Close()
		return fmt.Errorf("write webhook settings %s: %w", settingsPath, err)
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("write webhook settings %s: %w", settingsPath, err)
	}
	return nil
}

// Enabled reports whether repo has a configured webhook destination.
func (c *Client) Enabled(repo string) bool {
	if c == nil {
		return false
	}
	_, ok := c.repositories[repo]
	return ok
}

// Deliver sends a signed ProtoJSON push event. A delivery ID stays fixed over
// all attempts. The whole call is bounded by five seconds or the caller's
// earlier deadline. The caller decides how to handle a failed delivery.
func (c *Client) Deliver(ctx context.Context, repo string, event *pushv1.PushEvent) error {
	if !c.Enabled(repo) {
		return nil
	}
	if event == nil {
		return errors.New("webhook event is nil")
	}
	if event.GetEventId() == "" {
		return errors.New("webhook event ID is empty")
	}
	if event.GetRepository() != repo {
		return fmt.Errorf("webhook event repository %q does not match destination %q", event.GetRepository(), repo)
	}

	body, err := (protojson.MarshalOptions{EmitDefaultValues: true}).Marshal(event)
	if err != nil {
		return fmt.Errorf("marshal webhook event: %w", err)
	}
	dst := c.repositories[repo]
	mac := hmac.New(sha256.New, dst.secret)
	_, _ = mac.Write(body)
	signature := "sha256=" + hex.EncodeToString(mac.Sum(nil))

	ctx, cancel := context.WithTimeout(ctx, deliveryTimeout)
	defer cancel()
	var lastErr error
	for attempt := range maxAttempts {
		if err := ctx.Err(); err != nil {
			return fmt.Errorf("deliver webhook: %w", err)
		}
		retry, err := dst.deliverOnce(ctx, body, event.GetEventId(), signature)
		if err == nil {
			return nil
		}
		lastErr = err
		if !retry || attempt == maxAttempts-1 {
			break
		}
		timer := time.NewTimer(time.Duration(100<<attempt) * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return fmt.Errorf("deliver webhook: %w", ctx.Err())
		case <-timer.C:
		}
	}
	return fmt.Errorf("deliver webhook: %w", lastErr)
}

func (d destination) deliverOnce(ctx context.Context, body []byte, id, signature string) (bool, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, d.url, bytes.NewReader(body))
	if err != nil {
		return false, fmt.Errorf("create webhook request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Objgit-Event", "push")
	req.Header.Set("X-Objgit-Delivery", id)
	req.Header.Set("X-Objgit-Signature-256", signature)
	resp, err := d.client.Do(req)
	if err != nil {
		if ctx.Err() != nil {
			return false, ctx.Err()
		}
		// url.Error includes the full destination URL, which can contain
		// credentials in its query. Do not include that URL in returned errors.
		var urlErr *url.Error
		if errors.As(err, &urlErr) {
			err = urlErr.Err
		}
		return true, fmt.Errorf("request failed: %w", err)
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 4096))
	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		return false, nil
	}
	return resp.StatusCode == http.StatusTooManyRequests || resp.StatusCode >= 500,
		fmt.Errorf("destination returned HTTP %d", resp.StatusCode)
}

func validateURL(raw string) (*url.URL, bool, error) {
	u, err := url.Parse(raw)
	if err != nil {
		return nil, false, errors.New("invalid webhook URL")
	}
	if u.Hostname() == "" || u.User != nil || u.Fragment != "" || u.Opaque != "" {
		return nil, false, errors.New("webhook URL must have a host and no user info or fragment")
	}
	host := strings.TrimSuffix(strings.ToLower(u.Hostname()), ".")
	ip, parseErr := netip.ParseAddr(host)
	loopback := host == "localhost" || parseErr == nil && ip.Unmap().IsLoopback()
	if u.Scheme != "https" && !(u.Scheme == "http" && loopback) {
		return nil, false, errors.New("webhook URL must use HTTPS; HTTP is allowed only for loopback")
	}
	if parseErr == nil && !loopback && !publicIP(ip) {
		return nil, false, errors.New("webhook URL has a nonpublic IP address")
	}
	if port := u.Port(); port != "" {
		if _, err := net.LookupPort("tcp", port); err != nil {
			return nil, false, errors.New("webhook URL has an invalid port")
		}
	}
	return u, loopback, nil
}

func safeDialContext(expectedHost string, loopback bool) func(context.Context, string, string) (net.Conn, error) {
	return func(ctx context.Context, network, address string) (net.Conn, error) {
		host, port, err := net.SplitHostPort(address)
		if err != nil || !strings.EqualFold(strings.TrimSuffix(host, "."), strings.TrimSuffix(expectedHost, ".")) {
			return nil, errors.New("webhook destination changed")
		}
		ips, err := net.DefaultResolver.LookupNetIP(ctx, "ip", host)
		if err != nil {
			return nil, fmt.Errorf("resolve webhook destination: %w", err)
		}
		dialer := &net.Dialer{Timeout: attemptTimeout}
		var dialErr error
		for _, ip := range ips {
			if loopback && !ip.Unmap().IsLoopback() || !loopback && !publicIP(ip) {
				continue
			}
			conn, err := dialer.DialContext(ctx, network, net.JoinHostPort(ip.String(), port))
			if err == nil {
				return conn, nil
			}
			dialErr = err
		}
		if dialErr != nil {
			return nil, dialErr
		}
		return nil, errors.New("webhook destination resolved only to disallowed addresses")
	}
}

var nonpublicRanges = []netip.Prefix{
	netip.MustParsePrefix("100.64.0.0/10"),
	netip.MustParsePrefix("192.0.0.0/24"),
	netip.MustParsePrefix("192.0.2.0/24"),
	netip.MustParsePrefix("198.18.0.0/15"),
	netip.MustParsePrefix("198.51.100.0/24"),
	netip.MustParsePrefix("203.0.113.0/24"),
	netip.MustParsePrefix("224.0.0.0/4"),
	netip.MustParsePrefix("240.0.0.0/4"),
	netip.MustParsePrefix("2001:db8::/32"),
}

func publicIP(ip netip.Addr) bool {
	ip = ip.Unmap()
	if !ip.IsGlobalUnicast() || ip.IsPrivate() || ip.IsLoopback() || ip.IsLinkLocalUnicast() {
		return false
	}
	for _, prefix := range nonpublicRanges {
		if prefix.Contains(ip) {
			return false
		}
	}
	return true
}
