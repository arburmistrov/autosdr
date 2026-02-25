# Reconnect Campaign Automation (Gmail + Pipedrive)

## Что делает

- Берет подтвержденные контакты из `gmail_reconnect_approved.json`.
- Отправляет initial outreach из вашей Gmail почты.
- Если нет ответа, отправляет до 3 follow-up.
- На входящий ответ автоматически создает сделку в Pipedrive и добавляет note.

## Переменные окружения

```bash
export PIPEDRIVE_DOMAIN="yourcompany"
export PIPEDRIVE_API_TOKEN="..."
```

## 1) Инициализация очереди из approved JSON

```bash
python3 scripts/reconnect_campaign.py \
  --command init-queue \
  --approved-json /Users/arseniyburmistrov/Downloads/gmail_reconnect_approved.json \
  --state data/output/reconnect_campaign_state.json \
  --followup-gap-days 4 \
  --followup-max 3
```

## 2) Dry-run цикла (без отправки)

```bash
python3 scripts/reconnect_campaign.py \
  --command run-cycle \
  --state data/output/reconnect_campaign_state.json \
  --max-per-run 80
```

## 3) Реальная отправка (за 2–3 дня)

День 1 (пример: 80 писем):

```bash
python3 scripts/reconnect_campaign.py \
  --command run-cycle \
  --state data/output/reconnect_campaign_state.json \
  --max-per-run 80 \
  --send
```

День 2 (еще 80):

```bash
python3 scripts/reconnect_campaign.py \
  --command run-cycle \
  --state data/output/reconnect_campaign_state.json \
  --max-per-run 80 \
  --send
```

День 3 (остаток):

```bash
python3 scripts/reconnect_campaign.py \
  --command run-cycle \
  --state data/output/reconnect_campaign_state.json \
  --max-per-run 80 \
  --send
```

## 4) Ежедневный follow-up процесс

Запускать 1 раз в день:

```bash
python3 scripts/reconnect_campaign.py \
  --command run-cycle \
  --state data/output/reconnect_campaign_state.json \
  --max-per-run 120 \
  --send
```

Логика:
- сначала синхронизирует ответы;
- на новые ответы создает deal в Pipedrive;
- затем отправляет только due контакты без ответа;
- максимум 3 follow-up после initial.

## 5) Отчет

```bash
python3 scripts/reconnect_campaign.py \
  --command report \
  --state data/output/reconnect_campaign_state.json
```
