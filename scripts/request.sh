#!/bin/sh
FILE="${2:-}"
SERVER_ID="${3:-${SERVER_ID:-}}"
# URL="${2:-}"
TOKEN="${TOKEN:-}"

case $1 in
  ws)
    cat $FILE | jq -c  . | websocat ws://127.0.0.1:8081  | jq .
    ;;
  http)
    curl -X POST http://localhost:3000/upload \
      -H "Authorization: Bearer $TOKEN" \
      -F "server_id=$SERVER_ID" \
      -F "file=@$FILE" \
    ;;
  *)
    echo "Unkown command"
    echo "commands are [ws, http]"
esac
# if [$1=""] then;
# else if [$1=""] then;
# elif
#
