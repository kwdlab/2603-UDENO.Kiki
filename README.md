
# 2603-UDENO.Kiki




## 概要 (Overview)
本リポジトリは、HTTP/3(QUIC) ベンチマークツール（`quicbench`）の実装・利用手順をまとめたものです。  
HTTP/1.1・HTTP/2・HTTP/3 の各プロトコルで同条件のリクエストを行い、応答時間などの差を比較することを目的としています。

## ディスクリプション (Description)
### 目的
- HTTP/3(QUIC) のベンチマーク実行（並列リクエスト、繰り返し実行）
- HTTP/1.1 / HTTP/2 / HTTP/3 の比較実験を再現可能にする

### 使用ツール
- HTTP/3(QUIC): `h3bench`（本リポジトリの成果物）
- HTTP/1.1: `ab`（ApacheBench）
- HTTP/2: `curl`（HTTP/2 でヘッダ取得など）

## 開発・実行環境 (Build & Runtime Environment)
### 開発形態
- 手元PCから SSH 経由で学内サーバ `exe` に接続し、サーバ上でビルド・実行しました。

### 言語
- Go言語（`h3bench`）

 ## ライセンス (License)
Licensed under the New BSD License .




