#!/bin/bash

PORT=${1:-8000}
ACK_W=${2:-2}
ACK_R=${3:-2}
ITER=${4:-1000}

echo "======================================"
echo "=== Нагрузочный тест (wrk2) ==="
echo "Режим: ack_write=$ACK_W, ack_read=$ACK_R"
echo "======================================"

#Создаём Lua-скрипт для wrk2 с нужными ack
cat > /tmp/wrk_script.lua <<EOF
local ack_write = "$ACK_W"
local ack_read = "$ACK_R"
math.randomseed(os.time())
local body_value = string.rep("x", 1024)

request = function()
    local method_rand = math.random(1, 3)
    local method
    local body = nil
    local headers = {}
    local ack_param = ""

    if method_rand == 1 then
        method = "GET"
        ack_param = "&ack=" .. ack_read
    elseif method_rand == 2 then
        method = "PUT"
        ack_param = "&ack=" .. ack_write
    else
        method = "DELETE"
        ack_param = "&ack=" .. ack_write
    end

    local key = "key_" .. math.random(1, 1000000)

    if method == "PUT" then
        body = body_value .. os.time()
        headers["Content-Type"] = "application/octet-stream"
    end

    local uri = "/v0/entity?id=" .. key .. ack_param
    return wrk.format(method, uri, headers, body)
end
EOF

docker run --rm --network="host" \
  -v /tmp:/scripts \
  cylab/wrk2 \
  -t2 -c100 -R200 -d30s --latency \
  -s /scripts/wrk_script.lua \
  http://localhost:$PORT

echo ""
echo "======================================"
echo "=== Проверка консистентности (curl) ==="
echo "Количество проверок: $ITER"
echo "======================================"

#Проверка консистентности
SUCCESS=0
for i in $(seq 1 $ITER); do
    key="c_$RANDOM"
    value="val_$RANDOM"
    curl -s -X PUT -d "$value" "http://localhost:$PORT/v0/entity?id=$key&ack=2" > /dev/null
    sleep 0.02   #Небольшая пауза, чтобы репликация разошлась
    response=$(curl -s "http://localhost:$PORT/v0/entity?id=$key&ack=$ACK_R")
    if [ "$response" = "$value" ]; then
        ((SUCCESS++))
    fi
done

PERCENT=$(echo "scale=2; $SUCCESS*100/$ITER" | bc)
echo "Консистентность: $SUCCESS / $ITER ($PERCENT%)"