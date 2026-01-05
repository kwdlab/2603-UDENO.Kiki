// h3client.go
package main

import (
	"context"
	"crypto/tls"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/quic-go/quic-go/http3" // ＊新しい実装
)

/*
使い方（元の quicbench に概ね寄せています）:

  ./h3bench -u https://h3.exe.kwdlab.org/test100m.bin -c 10 -r 3      # 回数ベース
  ./h3bench -u https://h3.exe.kwdlab.org/test100m.bin -c 50 -t 30     # 時間ベース
  ./h3bench -u https://localhost:8443/100m -c 10 -r 3 -k              # 自己署名OK

対応フラグ:
  -u  target URL（必須）
  -c  併発クライアント数（既定 100）
  -r  各クライアントのリクエスト回数（既定 1、-t>0 のときは無視）
  -t  実行秒数（>0 のとき時間ベース）
  -k  証明書検証スキップ（自己署名など）
  -tc, -tr, -tw は互換ダミー（現状未使用）
*/

// struct でまとめる
type h3Config struct {
	URL       string
	Conc      int
	PerClient int
	Duration  int
	Insecure  bool
}

// struct を返す
func parseFlags() h3Config {
	u := flag.String("u", "", "target URL (e.g. https://h3.example.com/file)")
	c := flag.Int("c", 100, "concurrency (number of clients)")
	r := flag.Int("r", 1, "requests per client (ignored if -t>0)")
	t := flag.Int("t", -1, "test duration seconds (if >0, run by time)")
	_ = flag.Int("tc", 5000, "connect timeout (ms) [compat placeholder]")
	_ = flag.Int("tr", 5000, "read timeout (ms) [compat placeholder]")
	_ = flag.Int("tw", 5000, "write timeout (ms) [compat placeholder]")
	k := flag.Bool("k", false, "skip TLS verify (self-signed)") // ← 消えていたので復活
	flag.Parse()

	if strings.TrimSpace(*u) == "" {
		fmt.Fprintln(os.Stderr, "usage: h3bench -u <url> [-c 100] [-r 1] [-t seconds] [-k]")
		os.Exit(2)
	}

	return h3Config{
		URL:       *u,
		Conc:      *c,
		PerClient: *r,
		Duration:  *t,
		Insecure:  *k,
	}
}

// 1 リクエストごとの結果を oneResult にして channel で集計
type oneResult struct {
	ttfb  float64
	total float64
	err   error
}

func runH3Bench(cfg h3Config) error {
	transport := &http3.Transport{
		TLSClientConfig: &tls.Config{
			InsecureSkipVerify: cfg.Insecure,
			NextProtos:         []string{"h3"},
		},
	}
	defer transport.Close()

	client := &http.Client{Transport: transport}

	// グローバルカウンタは atomic で管理
	var (
		wg       sync.WaitGroup
		results  = make(chan oneResult, cfg.Conc*4)
		okCount  int64
		ngCount  int64
		sumTTFB  float64
		sumTotal float64
	)

	// ベンチ全体の時間（おまけ）
	startWall := time.Now()

	// 時間実行モードか回数モードか
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if cfg.Duration > 0 {
		go func() {
			time.Sleep(time.Duration(cfg.Duration) * time.Second)
			cancel()
		}()
	}

	worker := func(id int) {
		defer wg.Done()

		// 時間モードなら無限ループ（ctx キャンセルまで）
		n := cfg.PerClient
		if cfg.Duration > 0 {
			n = 1 << 30 // 擬似的に大きな回数
		}

		for i := 0; i < n; i++ {
			select {
			case <-ctx.Done():
				return
			default:
			}

			// ★ ここが一番大事：1 リクエストごとに時間を測る
			startReq := time.Now()
			rtStart := time.Now()

			req, err := http.NewRequest("GET", cfg.URL, nil)
			if err != nil {
				atomic.AddInt64(&ngCount, 1)
				results <- oneResult{err: err}
				continue
			}

			resp, err := client.Do(req)
			if err != nil {
				atomic.AddInt64(&ngCount, 1)
				results <- oneResult{err: err}
				continue
			}

			ttfb := time.Since(rtStart).Seconds()

			// 本文は捨てる（速度計測の都合で全量読む）
			_, _ = io.Copy(io.Discard, resp.Body)
			_ = resp.Body.Close()

			total := time.Since(startReq).Seconds()

			atomic.AddInt64(&okCount, 1)
			results <- oneResult{ttfb: ttfb, total: total, err: nil}
		}
	}

	wg.Add(cfg.Conc)
	for i := 0; i < cfg.Conc; i++ {
		go worker(i)
	}

	// 集計
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

loop:
	for {
		select {
		case r := <-results:
			if r.err == nil {
				sumTTFB += r.ttfb
				sumTotal += r.total
			}
		case <-done:
			break loop
		case <-ctx.Done():
			// 時間モードでキャンセルされた場合、残りの結果を吸い切る
			for {
				select {
				case r := <-results:
					if r.err == nil {
						sumTTFB += r.ttfb
						sumTotal += r.total
					}
				default:
					break loop
				}
			}
		}
	}

	ok := atomic.LoadInt64(&okCount)
	ng := atomic.LoadInt64(&ngCount)
	elapsed := time.Since(startWall).Seconds()

	if ok > 0 {
		avgTTFBms := (sumTTFB / float64(ok)) * 1000.0
		avgTotalms := (sumTotal / float64(ok)) * 1000.0
		fmt.Printf("Requests: %d  Success: %d  Fail: %d\n", ok+ng, ok, ng)
		fmt.Printf("TTFB avg = %.3f ms, Total avg = %.3f ms\n", avgTTFBms, avgTotalms)

		// test100m.bin のときだけおまけでスループット表示
		if strings.Contains(cfg.URL, "test100m.bin") {
			const bytesPerReq = 104857600.0 // 100MiB
			totalBytes := bytesPerReq * float64(ok)
			mbps := (totalBytes / 1024.0 / 1024.0 / elapsed) * 8.0
			fmt.Printf("Aggregate throughput ≒ %.2f Mbps\n", mbps)
		}
	} else {
		fmt.Printf("Requests: %d  Success: 0  Fail: %d\n", ok+ng, ng)
	}

	return nil
}

// 既存 main.go を触らずにこちらを main として使う場合は、
// このファイルを main パッケージに置き、次の main() を使います。
func main() {
	cfg := parseFlags()
	if err := runH3Bench(cfg); err != nil {
		fmt.Fprintln(os.Stderr, "error:", err) // 「エラーがあれば表示して終了」
		os.Exit(1)
	}
}
