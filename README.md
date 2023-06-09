# Distribute System Challenges

[![build](https://github.com/redsnow1992/dist-sys-rs/actions/workflows/build.yml/badge.svg)](https://github.com/redsnow1992/dist-sys-rs/actions/workflows/build.yml)

[challenge link](https://fly.io/dist-sys/)

## workloads

### echo
```
./maelstrom test -w echo --bin demo/ruby/echo.rb --time-limit 5 --node-count 10
```


### unique_id
```
./maelstrom test -w unique-ids --bin ~/go/bin/maelstrom-unique-ids --time-limit 30 --rate 1000 --node-count 3 --availability total --nemesis partition
```

### 3. broadcast
#### 3a: Single-Node Broadcast
```
./maelstrom test -w broadcast --bin ~/go/bin/maelstrom-broadcast --node-count 1 --time-limit 20 --rate 10
```
#### 3b: Multi-Node Broadcast
```
./maelstrom test -w broadcast --bin ~/go/bin/maelstrom-broadcast --node-count 5 --time-limit 20 --rate 10
```
#### 3c: Fault Tolerant Broadcast
```
./maelstrom test -w broadcast --bin ~/go/bin/maelstrom-broadcast --node-count 5 --time-limit 20 --rate 10 --nemesis partition
```
#### 3d: Efficient Broadcast, Part I
```
./maelstrom test -w broadcast --bin ~/go/bin/maelstrom-broadcast --node-count 25 --time-limit 20 --rate 100 --latency 100
```

## code coverage
```
cargo tarpaulin
```

## run precommit
```
pre-commit run --all-files
```

## todo
1. serde_json deserialize according message type
