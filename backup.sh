#!/bin/bash
BACKUP_DIR=/opt/popolnyaska-bot/backups
DB_FILE=/opt/popolnyaska-bot/orders.db
DATE=$(date +%Y-%m-%d_%H%M)

# SQLite online backup (safe even while bot is running)
sqlite3 "$DB_FILE" ".backup $BACKUP_DIR/orders_$DATE.db"

# Keep only last 7 backups
ls -t $BACKUP_DIR/orders_*.db 2>/dev/null | tail -n +8 | xargs -r rm

echo "[$DATE] Backup done, kept last 7"
