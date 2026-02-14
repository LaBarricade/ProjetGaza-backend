#!/bin/bash
dt=$(date '+%Y%m%d-%H%M%S');
npx supabase db dump -f backups/backup_schema_"$dt".sql
npx supabase db dump -f backups/backup_data_"$dt".sql --data-only