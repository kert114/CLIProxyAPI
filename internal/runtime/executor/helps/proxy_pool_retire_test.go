package helps

import (
	"bufio"
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestProxyPoolTLSRetire(t *testing.T) {
	source := httptest.NewTLSServer(http.NotFoundHandler())
	cert := source.TLS.Certificates[0]
	source.Close()
	servicePeer, clientPeer := net.Pipe()
	defer servicePeer.Close()
	defer clientPeer.Close()
	serverTLS := tls.Server(servicePeer, &tls.Config{Certificates: []tls.Certificate{cert}})
	serverResponse := make(chan error, 1)
	go func() {
		if _, err := http.ReadRequest(bufio.NewReader(serverTLS)); err != nil {
			serverResponse <- err
			return
		}
		_, err := fmt.Fprint(serverTLS, "HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok")
		serverResponse <- err
	}()
	publisher := RegisterProxyPoolPublisher()
	transport, err := proxyTransportCache.Get(proxyPoolKey{publisher: publisher, proxy: "fixture"}, func() (*http.Transport, error) {
		return &http.Transport{DialTLSContext: func(context.Context, string, string) (net.Conn, error) {
			return tls.Client(clientPeer, &tls.Config{InsecureSkipVerify: true}), nil
		}}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	response, err := (&http.Client{Transport: transport, Timeout: time.Second}).Get("https://fixture.invalid")
	if err != nil {
		t.Fatal(err)
	}
	_, err = io.ReadAll(response.Body)
	response.Body.Close()
	if err != nil {
		t.Fatal(err)
	}
	if err := <-serverResponse; err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	RetireProxyPoolPublisher(publisher)
	elapsed := time.Since(start)
	t.Logf("idle TLS pool retirement took %s", elapsed)
	if elapsed > time.Second {
		t.Fatal("retirement waited for the TLS close notification")
	}
}
