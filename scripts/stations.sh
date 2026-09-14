#!/usr/bin/env sh
# Station registry operations against the compose Postgres.
# Tokens are shown once, at mint time; only their sha256 hashes are stored.
set -eu

usage() {
    cat >&2 <<'USAGE'
usage: stations.sh <command> [args]
  add <name>                      register a station and mint its first token
  token-add <name>                mint an additional token (for rotation)
  token-revoke <name> <prefix>    revoke the token whose hash starts with <prefix> (see `tokens`)
  tokens <name>                   list a station's tokens
  revoke <name>                   revoke the station and every token
  list                            list stations with active token counts
USAGE
    exit 2
}

# psql interpolates :'var' from stdin (not from -c) and quotes it safely.
psql() {
    docker compose exec -T postgres psql -U postgres listening_post -v ON_ERROR_STOP=1 "$@"
}

mint() {
    token=$(openssl rand -hex 32)
    hash=$(printf %s "$token" | openssl dgst -sha256 | awk '{print $NF}')
    echo "INSERT INTO station_tokens (token_hash, station_id) VALUES (:'hash', :'name')" |
        psql -q -v name="$1" -v hash="$hash"
    echo "STATION_TOKEN=$token"
}

cmd=${1:-}
shift || true
case "$cmd" in
add)
    name=${1:?usage: add <name>}
    echo "INSERT INTO stations (id) VALUES (:'name')" | psql -q -v name="$name"
    mint "$name"
    ;;
token-add)
    mint "${1:?usage: token-add <name>}"
    ;;
token-revoke)
    name=${1:?usage: token-revoke <name> <prefix>}
    prefix=${2:?usage: token-revoke <name> <prefix>}
    n=$(echo "UPDATE station_tokens SET revoked_at = now()
               WHERE station_id = :'name' AND token_hash LIKE :'prefix' || '%' AND revoked_at IS NULL" |
        psql -v name="$name" -v prefix="$prefix" | sed -n 's/^UPDATE //p')  # not -q: we need the "UPDATE n" tag
    [ "$n" -gt 0 ] || { echo "no active token for $name matching $prefix" >&2; exit 1; }
    echo "revoked $n token(s)"
    ;;
tokens)
    echo "SELECT left(token_hash, 12) AS hash, created_at, revoked_at
          FROM station_tokens WHERE station_id = :'name' ORDER BY created_at" |
        psql -v name="${1:?usage: tokens <name>}"
    ;;
revoke)
    echo "UPDATE stations SET revoked_at = now() WHERE id = :'name'" |
        psql -q -v name="${1:?usage: revoke <name>}"
    ;;
list)
    psql -c "SELECT s.id, s.created_at, s.revoked_at,
                    count(t.token_hash) FILTER (WHERE t.revoked_at IS NULL) AS active_tokens
             FROM stations s LEFT JOIN station_tokens t ON t.station_id = s.id
             GROUP BY s.id ORDER BY s.id"
    ;;
*)
    usage
    ;;
esac
