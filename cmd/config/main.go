package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"regexp"
	"strconv"

	"github.com/patrickbucher/meow"
	"github.com/valkey-io/valkey-go"
)

func main() {
	addr := flag.String("addr", "0.0.0.0", "listen to address")
	port := flag.Uint("port", 8000, "listen on port")
	flag.Parse()

	log.SetOutput(os.Stderr)

	valkeyURL := os.Getenv("VALKEY_URL")
	if valkeyURL == "" {
		valkeyURL = "valkey.frickelcloud.ch:6379"
	}

	ctx := context.Background()
	client, err := valkey.NewClient(valkey.ClientOption{
		InitAddress: []string{"valkey.frickelcloud.ch:6379"},
		SelectDB:    11, // دیتابیس اختصاصی شما
	})
	if err != nil {
		log.Fatalf("failed to connect to valkey: %v", err)
	}
	defer client.Close()

	http.HandleFunc("/endpoints/", func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			getEndpoint(w, r, client, ctx)
		case http.MethodPost:
			postEndpoint(w, r, client, ctx)
		case http.MethodDelete:
			deleteEndpoint(w, r, client, ctx)
		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	})

	http.HandleFunc("/endpoints", func(w http.ResponseWriter, r *http.Request) {
		getEndpoints(w, r, client, ctx)
	})

	listenTo := fmt.Sprintf("%s:%d", *addr, *port)
	log.Printf("listen to %s", listenTo)
	log.Fatal(http.ListenAndServe(listenTo, nil))
}

func getEndpoint(w http.ResponseWriter, r *http.Request, vk valkey.Client, ctx context.Context) {
	identifier, err := extractEndpointIdentifier(r.URL.String())
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	data, err := vk.Do(ctx, vk.B().Hgetall().Key("endpoint:"+identifier).Build()).AsStrMap()
	if err != nil || len(data) == 0 {
		w.WriteHeader(http.StatusNotFound)
		return
	}

	sOnline, _ := strconv.Atoi(data["statusOnline"])
	fAfter, _ := strconv.Atoi(data["failAfter"])

	payload := meow.EndpointPayload{
		Identifier:   identifier,
		URL:          data["url"],
		Method:       data["method"],
		StatusOnline: uint16(sOnline),
		Frequency:    data["frequency"],
		FailAfter:    uint8(fAfter),
	}
	json.NewEncoder(w).Encode(payload)
}

func postEndpoint(w http.ResponseWriter, r *http.Request, vk valkey.Client, ctx context.Context) {
	var payload meow.EndpointPayload
	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	// اصلاح نهایی: استفاده از متد Mset برای ارسال نقشه فیلدها و مقادیر
	// روش جایگزین اگر بالایی خطا داد
	err := vk.Do(ctx, vk.B().Hset().
		Key("endpoint:"+payload.Identifier).
		FieldValue().
		FieldValue("url", payload.URL).
		FieldValue("method", payload.Method).
		FieldValue("statusOnline", strconv.Itoa(int(payload.StatusOnline))).
		FieldValue("frequency", payload.Frequency).
		FieldValue("failAfter", strconv.Itoa(int(payload.FailAfter))).
		Build()).Error()

	if err != nil {
		log.Printf("Valkey error: %v", err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusCreated)
}

func deleteEndpoint(w http.ResponseWriter, r *http.Request, vk valkey.Client, ctx context.Context) {
	identifier, err := extractEndpointIdentifier(r.URL.String())
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	vk.Do(ctx, vk.B().Del().Key("endpoint:"+identifier).Build())
	w.WriteHeader(http.StatusNoContent)
}

func getEndpoints(w http.ResponseWriter, r *http.Request, vk valkey.Client, ctx context.Context) {
	keys, err := vk.Do(ctx, vk.B().Keys().Pattern("endpoint:*").Build()).AsStrSlice()
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	payloads := make([]meow.EndpointPayload, 0)
	for _, key := range keys {
		data, _ := vk.Do(ctx, vk.B().Hgetall().Key(key).Build()).AsStrMap()
		sOnline, _ := strconv.Atoi(data["statusOnline"])
		fAfter, _ := strconv.Atoi(data["failAfter"])

		payloads = append(payloads, meow.EndpointPayload{
			Identifier:   key[9:],
			URL:          data["url"],
			Method:       data["method"],
			StatusOnline: uint16(sOnline),
			Frequency:    data["frequency"],
			FailAfter:    uint8(fAfter),
		})
	}
	json.NewEncoder(w).Encode(payloads)
}

var endpointIdentifierPattern = regexp.MustCompile(`^/endpoints/([a-z][-a-z0-9]+)$`)

func extractEndpointIdentifier(path string) (string, error) {
	matches := endpointIdentifierPattern.FindStringSubmatch(path)
	if len(matches) < 2 {
		return "", fmt.Errorf("invalid identifier")
	}
	return matches[1], nil
}