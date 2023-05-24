# Distribute System Challenges

[![build](https://github.com/redsnow1992/kv/actions/workflows/build.yml/badge.svg)](https://github.com/redsnow1992/kv/actions/workflows/build.yml)

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

## todo
1. serde_json deserialize according message type