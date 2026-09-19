#!/bin/sh
FILE="${2:-}"
SERVER_ID="${3:-${SERVER_ID:-}}"
SPACE_ID="${4:-${SPACE_ID:-}}"

TOKEN="${TOKEN:-}"
PORT="${PORT:-8081}"

WS_URL="ws://127.0.0.1:${PORT}"
HTTP_PORT="${HTTP_PORT:-3000}"
TOKEN_PATH=${TOKEN_PATH:-'.data.token.token'}

case $1 in
  login)
    export TOKEN=$(
      jq -c . /home/tinker/Programming/Rust/fsss/examples/requests/login.json \
      | websocat -n -1 "$WS_URL" \
      | jq -r "$TOKEN_PATH" 
    )
    echo "TOKEN=$TOKEN" > /home/tinker/Programming/Rust/fsss/.env
    ;;
  ws)
    source /home/tinker/Programming/Rust/fsss/.env
    cat "$FILE" \
    | jq -c --arg t "$TOKEN" '.payload |= with_entries(.value.token = $t)' \
    | jq -c . \
    | websocat -n "$WS_URL" \
    | jq .
    ;;
  http)
    curl -X POST "http://localhost:${HTTP_PORT}/upload" \
      -H "Authorization: Bearer $TOKEN" \
      -F "server_id=$SERVER_ID" \
      -F "space_id=$SPACE_ID" \
      -F "file=@$FILE"
    ;;
  *)
    echo "Unkown command"
    echo "commands are [ws, http]"
    ;;
esac
