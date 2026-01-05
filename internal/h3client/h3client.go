import (
    // 既存の import に加えて…
    "context"
    "example.com/quicbench-h3/internal/h3client"
)

func main() {
    // 既存のフラグ処理の後…
    ctx := context.Background()
    ttfbAvg, totalAvg, ok, ng, err := h3client.Run(ctx, h3client.Options{
        URL: *url, Concurrency: *c, Requests: *r, Insecure: *k,
    })
    if err != nil { panic(err) }
    fmt.Printf("Requests: %d  Success: %d  Fail: %d\n", *c**r, ok, ng)
    fmt.Printf("TTFB avg = %.3f ms, Total avg = %.3f ms\n",
        float64(ttfbAvg.Microseconds())/1000.0,
        float64(totalAvg.Microseconds())/1000.0)
}
