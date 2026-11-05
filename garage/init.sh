#!/usr/bin/env bash
# Однократная настройка Garage: layout из одного узла, ключ, бакеты raw и warehouse.
# Идемпотентен: повторный запуск ничего не ломает.
set -euo pipefail
cd "$(dirname "$0")/.."
set -a; source .env; set +a

g() { docker compose exec -T garage /garage "$@" 2>/dev/null; }

if g layout show | grep -q "Current cluster layout version: 0"; then
  g layout assign "$(g node id -q | cut -d@ -f1)" -z dc1 -c 20GB >/dev/null
  g layout apply --version 1 >/dev/null
fi
g key info lab >/dev/null || g key import --yes -n lab "$AWS_ACCESS_KEY_ID" "$AWS_SECRET_ACCESS_KEY" >/dev/null
for b in raw warehouse; do
  g bucket info "$b" >/dev/null || g bucket create "$b" >/dev/null
  g bucket allow --read --write --owner "$b" --key lab >/dev/null
done
g bucket list
