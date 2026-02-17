# Pipedrive Reactivation Automation

Цель: вернуть в работу лидов, которые не контактировались более 8 месяцев (240 дней), отправлять касания ежедневно из вашей почты и фиксировать успехи в вашем профиле Pipedrive.

## 1) Переменные окружения

```bash
export PIPEDRIVE_DOMAIN="yourcompany"
export PIPEDRIVE_API_TOKEN="..."

# Для реальной отправки почты (send mode)
export SMTP_HOST="smtp.office365.com"    # или smtp.gmail.com
export SMTP_PORT="587"
export SMTP_USERNAME="you@company.com"
export SMTP_PASSWORD="..."
export SMTP_SENDER="you@company.com"
```

## 2) Построить очередь stale-лидов (>240 дней)

```bash
python3 scripts/pipedrive_reactivation.py build-queue \
  --output data/output/pipedrive_reactivation_queue.csv \
  --stale-days 240
```

Результат: `data/output/pipedrive_reactivation_queue.csv` по всем пользователям Pipedrive.

## 3) Ежедневный прогон цепочки

### Очистка и приоритизация (обязательно перед отправкой)

```bash
python3 scripts/pipedrive_reactivation.py rank-queue \
  --queue data/output/pipedrive_reactivation_queue.csv \
  --output data/output/pipedrive_reactivation_queue_ranked.csv \
  --top-percent 20
```

Что делает:
- помечает мусор/нерелевантные записи (`keep_for_send=false`)
- считает `relevance_score`
- выделяет приоритетный сегмент `priority_bucket=top20`

### Dry-run (без отправки)
```bash
python3 scripts/pipedrive_reactivation.py send-daily \
  --queue data/output/pipedrive_reactivation_queue_ranked.csv \
  --daily-limit 20 \
  --stage-gap-days 4 \
  --signature "Arseniy Burmistrov\nBusiness Development, S-PRO" \
  --clean-only \
  --top-bucket-only \
  --top-percent 20
```

### Реальная отправка
```bash
python3 scripts/pipedrive_reactivation.py send-daily \
  --queue data/output/pipedrive_reactivation_queue_ranked.csv \
  --daily-limit 20 \
  --stage-gap-days 4 \
  --signature "Arseniy Burmistrov\nBusiness Development, S-PRO" \
  --clean-only \
  --top-bucket-only \
  --top-percent 20 \
  --send
```

Что делает `send-daily`:
- берет только контакты с `next_touch_date <= today`
- отправляет 1 письмо по текущей стадии (1->2->3)
- фиксирует note в Pipedrive по контакту (и текущей открытой сделке, если есть)
- двигает стадию и дату следующего касания

## 4) Когда клиент согласовал звонок

```bash
python3 scripts/pipedrive_reactivation.py booked-call \
  --person-id 12345 \
  --person-name "John Doe" \
  --deal-title "Reactivation - John Doe - AI Workshop" \
  --deal-value 15000 \
  --currency CHF \
  --call-date 2026-02-20 \
  --call-note "Booked from reactivation campaign" \
  --queue data/output/pipedrive_reactivation_queue.csv
```

Что делает команда:
- создает новую открытую сделку
- создает call activity на указанную дату
- пишет note в Pipedrive
- помечает контакт как `booked` в очереди

## 5) Рекомендация по ежедневному запуску

Поставить cron (каждый будний день, 09:00):

```bash
0 9 * * 1-5 cd "/Users/arseniyburmistrov/Documents/HSG OpenAI/S-PRO" && /usr/bin/python3 scripts/pipedrive_reactivation.py send-daily --queue data/output/pipedrive_reactivation_queue.csv --daily-limit 20 --stage-gap-days 4 --signature "Arseniy Burmistrov\nBusiness Development, S-PRO" --send
```

## Ограничения
- Для автоматического book-слота из почты нужна отдельная интеграция календаря + обработка входящих (IMAP/Webhook). В текущей версии `booked-call` вызывается после подтверждения вручную.
