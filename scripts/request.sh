#!/bin/sh
FILE="${2:-}"
SERVER_ID="${3:-${SERVER_ID:-}}"
SPACE_ID="${4:-${SPACE_ID:-}}"
# URL="${2:-}"
TOKEN="${TOKEN:-}"

case $1 in
  login)
    export TOKEN=$(jq -c . /home/tinker/Programming/Rust/fsss/examples/requests/login.json | websocat -n -1 ws://127.0.0.1:8081 | jq -r '.content.Token.token.token')
    echo "TOKEN=$TOKEN" > /home/tinker/Programming/Rust/fsss/.env
    # export $TOKEN
    ;;
  ws)
    source /home/tinker/Programming/Rust/fsss/.env
    cat $FILE | jq -c --arg t "$TOKEN" '.payload |= with_entries(.value.token = $t)' | jq -c  . | websocat -n  ws://127.0.0.1:8081  | jq .
    ;;
  http)
    curl -X POST http://localhost:3000/upload \
      -H "Authorization: Bearer $TOKEN" \
      -F "server_id=$SERVER_ID" \
      -F "space_id=$SPACE_ID" \
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
