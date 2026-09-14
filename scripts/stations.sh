#!/usr/bin/env sh
# Station registry operations against the compose Postgres.
# Stations are identified by UUID; any command taking <station> accepts an unambiguous prefix.
# Tokens are shown once, at mint time; only their sha256 hashes are stored.
set -eu

usage() {
    cat >&2 <<'USAGE'
usage: stations.sh <command> [args]
  add                               register a station and mint its first token
  token-add <station>               mint an additional token (for rotation)
  token-revoke <station> <prefix>   revoke the token whose hash starts with <prefix> (see `tokens`)
  tokens <station>                  list a station's tokens
  revoke <station>                  revoke the station and every token
  list                              list stations with active token counts
USAGE
    exit 2
}

# psql interpolates :'var' from stdin (not from -c) and quotes it safely.
psql() {
    docker compose exec -T postgres psql -U postgres listening_post -v ON_ERROR_STOP=1 "$@"
}

# resolve <uuid-or-prefix> → full uuid on stdout; fails unless exactly one station matches.
resolve() {
    matches=$(echo "SELECT id FROM stations WHERE id::text LIKE :'prefix' || '%'" | psql -qtA -v prefix="$1")
    case $(printf '%s\n' "$matches" | grep -c .) in
    1) echo "$matches" ;;
    0) echo "no station matching $1" >&2; exit 1 ;;
    *) printf 'ambiguous prefix %s:\n%s\n' "$1" "$matches" >&2; exit 1 ;;
    esac
}

mint() {
    token=$(openssl rand -hex 32)
    hash=$(printf %s "$token" | openssl dgst -sha256 | awk '{print $NF}')
    echo "INSERT INTO station_tokens (token_hash, station_id) VALUES (:'hash', :'id')" |
        psql -q -v id="$1" -v hash="$hash"
    echo "STATION_TOKEN=$token"
}

cmd=${1:-}
shift || true
case "$cmd" in
add)
    id=$(psql -qtAc "INSERT INTO stations DEFAULT VALUES RETURNING id")  # -q: drop the "INSERT 0 1" tag
    echo "STATION_ID=$id"
    mint "$id"
    ;;
token-add)
    id=$(resolve "${1:?usage: token-add <station>}")
    mint "$id"
    ;;
token-revoke)
    id=$(resolve "${1:?usage: token-revoke <station> <prefix>}")
    prefix=${2:?usage: token-revoke <station> <prefix>}
    n=$(echo "UPDATE station_tokens SET revoked_at = now()
               WHERE station_id = :'id' AND token_hash LIKE :'prefix' || '%' AND revoked_at IS NULL" |
        psql -v id="$id" -v prefix="$prefix" | sed -n 's/^UPDATE //p')  # not -q: we need the "UPDATE n" tag
    [ "$n" -gt 0 ] || { echo "no active token for $id matching $prefix" >&2; exit 1; }
    echo "revoked $n token(s)"
    ;;
tokens)
    id=$(resolve "${1:?usage: tokens <station>}")
    echo "SELECT left(token_hash, 12) AS hash, created_at, revoked_at
          FROM station_tokens WHERE station_id = :'id' ORDER BY created_at" | psql -v id="$id"
    ;;
revoke)
    id=$(resolve "${1:?usage: revoke <station>}")
    echo "UPDATE stations SET revoked_at = now() WHERE id = :'id'" | psql -q -v id="$id"
    ;;
list)
    psql -c "SELECT s.id, s.created_at, s.revoked_at,
                    count(t.token_hash) FILTER (WHERE t.revoked_at IS NULL) AS active_tokens
             FROM stations s LEFT JOIN station_tokens t ON t.station_id = s.id
             GROUP BY s.id ORDER BY s.created_at"
    ;;
*)
    usage
    ;;
esac
